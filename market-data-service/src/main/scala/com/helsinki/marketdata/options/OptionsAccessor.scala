package com.helsinki.marketdata.options

import com.helsinki.marketdata.options.OptionsRecordCodec.given
import io.circe.*
import io.circe.parser.*
import io.circe.syntax.*
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/** Generic accessor for OptionsRecord data in Redis.
  * One hash per contract: options:{OCC_SYMBOL}
  * Sub-attributes (pricing, greeks, sizing, pnl) stored as JSON values.
  * Identity and order fields stored as flat strings.
  */
trait OptionsAccessor:
  def get(symbol: String): Option[OptionsRecord]
  def getByUnderlying(underlying: String): Seq[OptionsRecord]
  def listSymbols(): Seq[String]
  def put(record: OptionsRecord): Unit
  def putQuote(record: OptionsRecord): Unit
  def putPosition(record: OptionsRecord): Unit
  def putOrders(symbol: String, orders: Seq[OrderEntry]): Unit
  def putBars(symbol: String, bars: Seq[ContractBar]): Unit
  def close(): Unit


class RedisOptionsAccessor(
  redisHost: String,
  redisPort: Int,
  redisPassword: String
) extends OptionsAccessor:

  private val KeyPrefix = "options"
  private val IndexPrefix = "options:index"
  private val AllIndex = s"$IndexPrefix:all"

  private val poolConfig = new JedisPoolConfig()
  poolConfig.setMaxTotal(4)
  poolConfig.setMaxIdle(2)

  private val pool = new JedisPool(
    poolConfig, redisHost, redisPort, 5000,
    if redisPassword.nonEmpty then redisPassword else null
  )

  private def keyFor(symbol: String): String = s"$KeyPrefix:$symbol"

  override def get(symbol: String): Option[OptionsRecord] =
    val jedis = pool.getResource
    try
      val raw = jedis.hgetAll(keyFor(symbol))
      if raw.isEmpty || !raw.containsKey("symbol") then None
      else fromRedisHash(raw)
    finally
      jedis.close()

  override def getByUnderlying(underlying: String): Seq[OptionsRecord] =
    val jedis = pool.getResource
    try
      val symbols = jedis.smembers(s"$IndexPrefix:$underlying")
      if symbols == null || symbols.isEmpty then Seq.empty
      else
        import scala.jdk.CollectionConverters.*
        symbols.asScala.toSeq.sorted.flatMap(sym => get(sym))
    finally
      jedis.close()

  override def listSymbols(): Seq[String] =
    val jedis = pool.getResource
    try
      val members = jedis.smembers(AllIndex)
      if members == null then Seq.empty
      else
        import scala.jdk.CollectionConverters.*
        members.asScala.toSeq.sorted
    finally
      jedis.close()

  override def putQuote(record: OptionsRecord): Unit =
    val jedis = pool.getResource
    try
      val pipe = jedis.pipelined()
      val key = keyFor(record.symbol)

      writeIdentity(pipe, key, record)
      pipe.hset(key, "pricing", record.pricing.asJson.noSpaces)
      pipe.hset(key, "greeks", record.greeks.asJson.noSpaces)
      pipe.hset(key, "sizing", record.sizing.asJson.noSpaces)
      pipe.hset(key, "updated_at", java.time.Instant.now.toString)

      pipe.sadd(AllIndex, record.symbol)
      pipe.sadd(s"$IndexPrefix:${record.underlying}", record.symbol)
      pipe.sync()
    finally
      jedis.close()

  override def putPosition(record: OptionsRecord): Unit =
    val jedis = pool.getResource
    try
      val pipe = jedis.pipelined()
      val key = keyFor(record.symbol)

      writeIdentity(pipe, key, record)
      pipe.hset(key, "pricing", record.pricing.asJson.noSpaces)
      pipe.hset(key, "sizing", record.sizing.asJson.noSpaces)
      pipe.hset(key, "pnl", record.pnl.asJson.noSpaces)
      pipe.hset(key, "updated_at", java.time.Instant.now.toString)

      pipe.sadd(AllIndex, record.symbol)
      pipe.sadd(s"$IndexPrefix:${record.underlying}", record.symbol)
      pipe.sync()
    finally
      jedis.close()

  override def putOrders(symbol: String, orders: Seq[OrderEntry]): Unit =
    val jedis = pool.getResource
    try
      jedis.hset(keyFor(symbol), "orders", Json.fromValues(orders.map(_.asJson)).noSpaces)
      jedis.hset(keyFor(symbol), "updated_at", java.time.Instant.now.toString)
    finally
      jedis.close()

  override def putBars(symbol: String, bars: Seq[ContractBar]): Unit =
    val jedis = pool.getResource
    try
      jedis.hset(keyFor(symbol), "bars", Json.fromValues(bars.map(_.asJson)).noSpaces)
      jedis.hset(keyFor(symbol), "updated_at", java.time.Instant.now.toString)
    finally
      jedis.close()

  override def put(record: OptionsRecord): Unit =
    val jedis = pool.getResource
    try
      val pipe = jedis.pipelined()
      val key = keyFor(record.symbol)

      writeIdentity(pipe, key, record)
      pipe.hset(key, "pricing", record.pricing.asJson.noSpaces)
      pipe.hset(key, "greeks", record.greeks.asJson.noSpaces)
      pipe.hset(key, "sizing", record.sizing.asJson.noSpaces)
      pipe.hset(key, "pnl", record.pnl.asJson.noSpaces)

      record.side.foreach(v => pipe.hset(key, "side", v))
      record.orderType.foreach(v => pipe.hset(key, "order_type", v))
      record.status.foreach(v => pipe.hset(key, "status", v))
      record.orderId.foreach(v => pipe.hset(key, "order_id", v))

      if record.orders.nonEmpty then
        pipe.hset(key, "orders", Json.fromValues(record.orders.map(_.asJson)).noSpaces)
      if record.bars.nonEmpty then
        pipe.hset(key, "bars", Json.fromValues(record.bars.map(_.asJson)).noSpaces)

      pipe.hset(key, "updated_at", java.time.Instant.now.toString)
      if record.createdAt.nonEmpty then pipe.hset(key, "created_at", record.createdAt)

      pipe.sadd(AllIndex, record.symbol)
      pipe.sadd(s"$IndexPrefix:${record.underlying}", record.symbol)
      pipe.sync()
    finally
      jedis.close()

  override def close(): Unit = pool.close()

  private def writeIdentity(pipe: redis.clients.jedis.Pipeline, key: String, r: OptionsRecord): Unit =
    pipe.hset(key, "symbol", r.symbol)
    pipe.hset(key, "underlying", r.underlying)
    pipe.hset(key, "expiration", r.expiration)
    pipe.hset(key, "strike", r.strike.toString)
    pipe.hset(key, "option_type", r.optionType)

  private def fromRedisHash(raw: java.util.Map[String, String]): Option[OptionsRecord] =
    import scala.jdk.CollectionConverters.*
    val m = raw.asScala

    m.get("symbol").map { symbol =>
      val pricing = m.get("pricing").flatMap(j => parse(j).toOption.flatMap(_.as[Pricing].toOption)).getOrElse(Pricing())
      val greeks = m.get("greeks").flatMap(j => parse(j).toOption.flatMap(_.as[Greeks].toOption)).getOrElse(Greeks())
      val sizing = m.get("sizing").flatMap(j => parse(j).toOption.flatMap(_.as[Sizing].toOption)).getOrElse(Sizing())
      val pnl = m.get("pnl").flatMap(j => parse(j).toOption.flatMap(_.as[PnL].toOption)).getOrElse(PnL())
      val orders = m.get("orders").flatMap(j => parse(j).toOption.flatMap(_.as[Seq[OrderEntry]].toOption)).getOrElse(Seq.empty)
      val bars = m.get("bars").flatMap(j => parse(j).toOption.flatMap(_.as[Seq[ContractBar]].toOption)).getOrElse(Seq.empty)

      OptionsRecord(
        symbol = symbol,
        underlying = m.getOrElse("underlying", ""),
        expiration = m.getOrElse("expiration", ""),
        strike = m.get("strike").flatMap(_.toDoubleOption).getOrElse(0.0),
        optionType = m.getOrElse("option_type", ""),
        pricing = pricing, greeks = greeks, sizing = sizing, pnl = pnl,
        side = m.get("side"), orderType = m.get("order_type"),
        status = m.get("status"), orderId = m.get("order_id"),
        orders = orders, bars = bars,
        updatedAt = m.getOrElse("updated_at", ""),
        createdAt = m.getOrElse("created_at", "")
      )
    }

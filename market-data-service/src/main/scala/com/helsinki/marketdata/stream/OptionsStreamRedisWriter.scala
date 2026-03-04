package com.helsinki.marketdata.stream

import com.helsinki.marketdata.config.AppConfig
import com.helsinki.marketdata.stream.OptionsStreamEncoders.given
import io.circe.Json
import io.circe.syntax.*
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

class OptionsStreamRedisWriter(config: AppConfig):
  private val TradesKey       = "options-stream:trades"
  private val QuotesKey       = "options-stream:quotes"
  private val QuotesHistKey   = "options-stream:quotes:history"
  private val LatestKey       = "options-stream:latest"
  private val MetaKey         = "options-stream:_meta"

  private val poolConfig = new JedisPoolConfig()
  poolConfig.setMaxTotal(4)
  poolConfig.setMaxIdle(2)

  private val pool = new JedisPool(
    poolConfig,
    config.optionsStreamRedisHost,
    config.optionsStreamRedisPort,
    5000,
    if config.optionsStreamRedisPassword.nonEmpty then config.optionsStreamRedisPassword else null
  )

  def writeTradeEvent(event: OptionTradeEvent): Unit =
    val jedis = pool.getResource
    try
      val pipe = jedis.pipelined()
      val json = event.asJson.noSpaces

      // Append to trades list
      pipe.lpush(s"$TradesKey:${event.symbol}", json)
      pipe.ltrim(s"$TradesKey:${event.symbol}", 0, config.historyMaxSize - 1)

      // Update latest snapshot
      pipe.hset(s"$LatestKey:${event.symbol}", "last_trade", json)
      pipe.hset(s"$LatestKey:${event.symbol}", "last_trade_at", event.timestamp)

      // Update meta
      pipe.hset(MetaKey, "last_event_at", java.time.Instant.now.toString)
      pipe.hset(MetaKey, "last_trade_at", event.timestamp)

      pipe.sync()
    finally
      jedis.close()

  def writeQuoteEvent(event: OptionQuoteEvent): Unit =
    val jedis = pool.getResource
    try
      val pipe = jedis.pipelined()
      val json = event.asJson.noSpaces

      // Update latest quote hash
      val quoteKey = s"$QuotesKey:${event.symbol}"
      pipe.hset(quoteKey, "bid_price", event.bidPrice.toString)
      pipe.hset(quoteKey, "ask_price", event.askPrice.toString)
      pipe.hset(quoteKey, "bid_size", event.bidSize.toString)
      pipe.hset(quoteKey, "ask_size", event.askSize.toString)
      pipe.hset(quoteKey, "bid_exchange", event.bidExchange)
      pipe.hset(quoteKey, "ask_exchange", event.askExchange)
      pipe.hset(quoteKey, "timestamp", event.timestamp)
      pipe.hset(quoteKey, "mid", ((event.bidPrice + event.askPrice) / 2.0).toString)
      pipe.hset(quoteKey, "spread", (event.askPrice - event.bidPrice).toString)

      // Append to quote history
      pipe.lpush(s"$QuotesHistKey:${event.symbol}", json)
      pipe.ltrim(s"$QuotesHistKey:${event.symbol}", 0, config.historyMaxSize - 1)

      // Update latest snapshot
      pipe.hset(s"$LatestKey:${event.symbol}", "last_quote", json)
      pipe.hset(s"$LatestKey:${event.symbol}", "last_quote_at", event.timestamp)

      // Update meta
      pipe.hset(MetaKey, "last_event_at", java.time.Instant.now.toString)
      pipe.hset(MetaKey, "last_quote_at", event.timestamp)

      pipe.sync()
    finally
      jedis.close()

  def writeGreeks(symbol: String, greeksJson: String): Unit =
    val jedis = pool.getResource
    try
      jedis.hset(s"$LatestKey:$symbol", "greeks", greeksJson)
      jedis.hset(s"$LatestKey:$symbol", "greeks_updated_at", java.time.Instant.now.toString)
    finally
      jedis.close()

  def writeMeta(symbol: String, feed: String): Unit =
    val jedis = pool.getResource
    try
      val pipe = jedis.pipelined()
      pipe.hset(MetaKey, "symbol", symbol)
      pipe.hset(MetaKey, "feed", feed)
      pipe.hset(MetaKey, "connected_at", java.time.Instant.now.toString)
      pipe.hset(MetaKey, "status", "connected")
      pipe.sync()
    finally
      jedis.close()

  def writeStatus(status: String): Unit =
    val jedis = pool.getResource
    try
      jedis.hset(MetaKey, "status", status)
      jedis.hset(MetaKey, "status_at", java.time.Instant.now.toString)
    finally
      jedis.close()

  def ping(): Boolean =
    val jedis = pool.getResource
    try
      jedis.ping() == "PONG"
    catch
      case _: Exception => false
    finally
      jedis.close()

  def close(): Unit = pool.close()

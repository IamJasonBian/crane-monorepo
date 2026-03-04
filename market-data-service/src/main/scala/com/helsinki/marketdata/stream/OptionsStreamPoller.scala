package com.helsinki.marketdata.stream

import com.helsinki.marketdata.config.AppConfig
import com.helsinki.marketdata.options.{OptionsClient, OptionsEncoders}
import io.circe.Json
import io.circe.syntax.*
import java.time.LocalDate
import java.time.format.DateTimeFormatter

class OptionsStreamPoller(config: AppConfig):
  private val redis = OptionsStreamRedisWriter(config)
  private val snapshotClient = OptionsClient(config)
  @volatile private var running = true
  private var streamClient: OptionsStreamClient = null

  def start(): Unit =
    val symbol = buildOccSymbol(
      config.optionsStreamTicker,
      config.optionsStreamExpiration,
      config.optionsStreamType,
      config.optionsStreamStrike
    )

    println(s"[options-stream] Starting options stream")
    println(s"  ticker:     ${config.optionsStreamTicker}")
    println(s"  expiration: ${config.optionsStreamExpiration}")
    println(s"  strike:     ${config.optionsStreamStrike}")
    println(s"  type:       ${config.optionsStreamType}")
    println(s"  OCC symbol: $symbol")
    println(s"  feed:       ${config.optionsStreamFeed}")
    println(s"  redis:      ${config.optionsStreamRedisHost}:${config.optionsStreamRedisPort}")

    // Verify Redis
    try
      if redis.ping() then
        println("[options-stream] Redis connection OK")
      else
        println("[options-stream] WARNING: Redis ping failed")
    catch
      case e: Exception =>
        println(s"[options-stream] WARNING: Redis ping error: ${e.getMessage}")

    redis.writeMeta(symbol, config.optionsStreamFeed)

    // Start supplementary greeks polling on a separate thread
    val greeksThread = new Thread(() => pollGreeks(symbol), "options-stream-greeks")
    greeksThread.setDaemon(true)
    greeksThread.start()

    // Start WebSocket stream (blocks on reconnect loop)
    streamClient = OptionsStreamClient(
      config = config,
      contractSymbol = symbol,
      onTrade = handleTrade,
      onQuote = handleQuote
    )
    streamClient.connect() // Blocks until stop() is called

  private def handleTrade(event: OptionTradeEvent): Unit =
    try
      redis.writeTradeEvent(event)
    catch
      case e: Exception =>
        println(s"[options-stream] Redis trade write error: ${e.getMessage}")

  private def handleQuote(event: OptionQuoteEvent): Unit =
    try
      redis.writeQuoteEvent(event)
    catch
      case e: Exception =>
        println(s"[options-stream] Redis quote write error: ${e.getMessage}")

  /** Poll for greeks/IV via the snapshot REST API every 30s. */
  private def pollGreeks(symbol: String): Unit =
    import OptionsEncoders.given
    println(s"[options-stream] Starting greeks poller for $symbol")

    while running do
      try
        val snapshots = snapshotClient.fetchOptionSnapshots(Seq(symbol))
        snapshots.headOption.foreach { snap =>
          val greeksJson = Json.obj(
            "symbol" -> Json.fromString(snap.symbol),
            "greeks" -> snap.greeks.fold(Json.Null)(_.asJson),
            "implied_volatility" -> snap.impliedVolatility.fold(Json.Null)(Json.fromDoubleOrNull),
            "latest_trade" -> snap.latestTrade.fold(Json.Null)(t => Json.obj(
              "price" -> Json.fromDoubleOrNull(t.price),
              "size" -> Json.fromInt(t.size),
              "timestamp" -> Json.fromString(t.timestamp)
            )),
            "latest_quote" -> snap.latestQuote.fold(Json.Null)(q => Json.obj(
              "bid" -> Json.fromDoubleOrNull(q.bid),
              "ask" -> Json.fromDoubleOrNull(q.ask),
              "bid_size" -> Json.fromInt(q.bidSize),
              "ask_size" -> Json.fromInt(q.askSize),
              "timestamp" -> Json.fromString(q.timestamp)
            ))
          ).noSpaces
          redis.writeGreeks(symbol, greeksJson)
        }
      catch
        case e: Exception =>
          println(s"[options-stream] Greeks poll error: ${e.getMessage}")

      Thread.sleep(30000) // 30 seconds

  /** Build OCC contract symbol: AAPL250620C00150000 */
  private def buildOccSymbol(ticker: String, expiration: String, optionType: String, strike: Double): String =
    val date = LocalDate.parse(expiration)
    val dateStr = date.format(DateTimeFormatter.ofPattern("yyMMdd"))
    val strikeInt = (strike * 1000).toLong
    val strikeStr = f"$strikeInt%08d"
    s"${ticker.toUpperCase}${dateStr}${optionType.toUpperCase}${strikeStr}"

  def stop(): Unit =
    running = false
    if streamClient != null then streamClient.stop()
    snapshotClient.close()
    redis.writeStatus("disconnected")
    redis.close()
    println("[options-stream] Stopped")

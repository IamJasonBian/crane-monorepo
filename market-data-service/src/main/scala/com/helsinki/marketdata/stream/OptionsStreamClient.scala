package com.helsinki.marketdata.stream

import com.helsinki.marketdata.config.AppConfig
import org.msgpack.core.{MessagePack, MessageUnpacker}
import org.msgpack.value.ValueType
import java.net.URI
import java.net.http.{HttpClient, WebSocket}
import java.nio.ByteBuffer
import java.util.concurrent.{CompletionStage, CountDownLatch}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

class OptionsStreamClient(
  config: AppConfig,
  contractSymbol: String,
  onTrade: OptionTradeEvent => Unit,
  onQuote: OptionQuoteEvent => Unit,
  additionalSymbols: Seq[String] = Seq.empty
):
  private val wsUrl = s"wss://stream.data.alpaca.markets/v1beta1/${config.optionsStreamFeed}"
  @volatile private var ws: WebSocket = null
  private val connected = AtomicBoolean(false)
  private val shouldRun = AtomicBoolean(true)
  private val lastEventAt = AtomicLong(0L)

  // Buffer for accumulating binary frames
  private val binaryBuffer = java.io.ByteArrayOutputStream()

  def connect(): Unit =
    println(s"[options-stream] Connecting to $wsUrl")
    val allSymbols = contractSymbol +: additionalSymbols
    println(s"[options-stream] Contracts: ${allSymbols.mkString(", ")}")

    val httpClient = HttpClient.newHttpClient()
    var backoff = 1000L

    while shouldRun.get() do
      try
        val latch = CountDownLatch(1)

        ws = httpClient.newWebSocketBuilder()
          .buildAsync(URI.create(wsUrl), new WebSocket.Listener:
            override def onOpen(webSocket: WebSocket): Unit =
              println("[options-stream] WebSocket connected")
              latch.countDown()
              webSocket.request(1)

            override def onBinary(webSocket: WebSocket, data: ByteBuffer, last: Boolean): CompletionStage[?] =
              val bytes = new Array[Byte](data.remaining())
              data.get(bytes)
              binaryBuffer.write(bytes)

              if last then
                try
                  val fullMessage = binaryBuffer.toByteArray
                  binaryBuffer.reset()
                  handleMessage(fullMessage)
                catch
                  case e: Exception =>
                    println(s"[options-stream] Message parse error: ${e.getMessage}")

              webSocket.request(1)
              null

            override def onText(webSocket: WebSocket, data: CharSequence, last: Boolean): CompletionStage[?] =
              // Fallback for text frames (shouldn't happen with options stream)
              println(s"[options-stream] Unexpected text frame: $data")
              webSocket.request(1)
              null

            override def onClose(webSocket: WebSocket, statusCode: Int, reason: String): CompletionStage[?] =
              println(s"[options-stream] WebSocket closed: $statusCode $reason")
              connected.set(false)
              null

            override def onError(webSocket: WebSocket, error: Throwable): Unit =
              println(s"[options-stream] WebSocket error: ${error.getMessage}")
              connected.set(false)
          ).join()

        latch.await()
        backoff = 1000L // Reset backoff on successful connect

        // Wait for disconnect (the listener handles auth and subscription via handleMessage)
        while connected.get() && shouldRun.get() do
          Thread.sleep(1000)

        if !shouldRun.get() then return

      catch
        case e: Exception =>
          println(s"[options-stream] Connection error: ${e.getMessage}")

      // Reconnect with exponential backoff
      if shouldRun.get() then
        println(s"[options-stream] Reconnecting in ${backoff}ms...")
        Thread.sleep(backoff)
        backoff = Math.min(backoff * 2, 60000L)

  private def handleMessage(data: Array[Byte]): Unit =
    val unpacker = MessagePack.newDefaultUnpacker(data)
    try
      // Messages arrive as an array of maps
      val arrayLen = unpacker.unpackArrayHeader()
      var i = 0
      while i < arrayLen do
        val msg = unpackMap(unpacker)
        msg.get("T") match
          case Some("success") =>
            handleSuccess(msg)
          case Some("error") =>
            handleError(msg)
          case Some("subscription") =>
            handleSubscription(msg)
          case Some("t") =>
            handleTradeMessage(msg)
          case Some("q") =>
            handleQuoteMessage(msg)
          case Some(other) =>
            println(s"[options-stream] Unknown message type: $other")
          case None =>
            println(s"[options-stream] Message without type field")
        i += 1
    catch
      case e: Exception =>
        println(s"[options-stream] Unpack error: ${e.getMessage}")
    finally
      unpacker.close()

  private def unpackMap(unpacker: MessageUnpacker): Map[String, Any] =
    val mapLen = unpacker.unpackMapHeader()
    val builder = Map.newBuilder[String, Any]
    var j = 0
    while j < mapLen do
      val key = unpacker.unpackString()
      val fmt = unpacker.getNextFormat
      val value: Any = fmt.getValueType match
        case ValueType.STRING => unpacker.unpackString()
        case ValueType.INTEGER => unpacker.unpackLong()
        case ValueType.FLOAT => unpacker.unpackDouble()
        case ValueType.BOOLEAN => unpacker.unpackBoolean()
        case ValueType.NIL => unpacker.unpackNil(); null
        case ValueType.ARRAY =>
          val arrLen = unpacker.unpackArrayHeader()
          val arr = (0 until arrLen).map { _ =>
            val f = unpacker.getNextFormat
            f.getValueType match
              case ValueType.STRING => unpacker.unpackString()
              case _ => { unpacker.skipValue(); null }
          }.toSeq
          arr
        case _ =>
          unpacker.skipValue()
          null
      builder += (key -> value)
      j += 1
    builder.result()

  private def handleSuccess(msg: Map[String, Any]): Unit =
    msg.get("msg") match
      case Some("connected") =>
        println("[options-stream] Server: connected, authenticating...")
        sendAuth()
      case Some("authenticated") =>
        println("[options-stream] Server: authenticated, subscribing...")
        connected.set(true)
        sendSubscribe()
      case Some(other) =>
        println(s"[options-stream] Server success: $other")
      case None =>
        println("[options-stream] Server success (no message)")

  private def handleError(msg: Map[String, Any]): Unit =
    val code = msg.get("code").map(_.toString).getOrElse("?")
    val message = msg.get("msg").map(_.toString).getOrElse("unknown")
    println(s"[options-stream] ERROR ($code): $message")

    // Fatal errors: stop reconnecting
    if code == "402" then
      println("[options-stream] FATAL: Auth failed — check ALPACA_API_KEY and ALPACA_SECRET_KEY")
      shouldRun.set(false)

  private def handleSubscription(msg: Map[String, Any]): Unit =
    println(s"[options-stream] Subscribed — trades: ${msg.getOrElse("trades", "[]")}, quotes: ${msg.getOrElse("quotes", "[]")}")

  private def handleTradeMessage(msg: Map[String, Any]): Unit =
    try
      val event = OptionTradeEvent(
        symbol    = msg.getOrElse("S", "").toString,
        timestamp = msg.getOrElse("t", "").toString,
        price     = toDouble(msg.getOrElse("p", 0.0)),
        size      = toInt(msg.getOrElse("s", 0)),
        exchange  = msg.getOrElse("x", "").toString,
        condition = msg.getOrElse("c", "").toString
      )
      lastEventAt.set(System.currentTimeMillis())
      onTrade(event)
    catch
      case e: Exception =>
        println(s"[options-stream] Trade parse error: ${e.getMessage}")

  private def handleQuoteMessage(msg: Map[String, Any]): Unit =
    try
      val condition = msg.get("c") match
        case Some(seq: Seq[?]) => seq.headOption.map(_.toString).getOrElse("")
        case Some(s)           => s.toString
        case None              => ""

      val event = OptionQuoteEvent(
        symbol      = msg.getOrElse("S", "").toString,
        timestamp   = msg.getOrElse("t", "").toString,
        bidExchange = msg.getOrElse("bx", "").toString,
        bidPrice    = toDouble(msg.getOrElse("bp", 0.0)),
        bidSize     = toInt(msg.getOrElse("bs", 0)),
        askExchange = msg.getOrElse("ax", "").toString,
        askPrice    = toDouble(msg.getOrElse("ap", 0.0)),
        askSize     = toInt(msg.getOrElse("as", 0)),
        condition   = condition
      )
      lastEventAt.set(System.currentTimeMillis())
      onQuote(event)
    catch
      case e: Exception =>
        println(s"[options-stream] Quote parse error: ${e.getMessage}")

  private def sendAuth(): Unit =
    val json = s"""{"action":"auth","key":"${config.alpacaApiKey}","secret":"${config.alpacaSecretKey}"}"""
    sendText(json)

  private def sendSubscribe(): Unit =
    val allSymbols = contractSymbol +: additionalSymbols
    val symbolList = allSymbols.map(s => s""""$s"""").mkString(",")
    val json = s"""{"action":"subscribe","trades":[$symbolList],"quotes":[$symbolList]}"""
    sendText(json)

  private def sendText(text: String): Unit =
    try
      // Encode to msgpack
      val packer = MessagePack.newDefaultBufferPacker()
      // For auth and subscribe, we send as msgpack-encoded JSON string
      // Actually, Alpaca accepts either JSON text or msgpack for outbound messages
      // Using text is simpler and universally supported
      ws.sendText(text, true).join()
    catch
      case e: Exception =>
        println(s"[options-stream] Send error: ${e.getMessage}")

  private def toDouble(v: Any): Double = v match
    case d: Double => d
    case l: Long   => l.toDouble
    case f: Float  => f.toDouble
    case i: Int    => i.toDouble
    case s: String => s.toDoubleOption.getOrElse(0.0)
    case _         => 0.0

  private def toInt(v: Any): Int = v match
    case i: Int    => i
    case l: Long   => l.toInt
    case d: Double => d.toInt
    case s: String => s.toIntOption.getOrElse(0)
    case _         => 0

  def isConnected: Boolean = connected.get()

  def stop(): Unit =
    shouldRun.set(false)
    connected.set(false)
    if ws != null then
      try
        ws.sendClose(WebSocket.NORMAL_CLOSURE, "shutdown").join()
      catch
        case _: Exception => ()
    println("[options-stream] Client stopped")

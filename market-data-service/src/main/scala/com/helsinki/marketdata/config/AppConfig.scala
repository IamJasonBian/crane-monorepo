package com.helsinki.marketdata.config

case class AppConfig(
  alpacaApiKey: String,
  alpacaSecretKey: String,
  redisHost: String,
  redisPort: Int,
  redisPassword: String,
  pollIntervalMs: Long,
  symbols: Seq[String],
  cryptoSymbols: Seq[String],
  historyMaxSize: Int,
  optionsSymbols: Seq[String],
  optionsPollIntervalMs: Long,
  // Options WebSocket stream config
  optionsStreamEnabled: Boolean,
  optionsStreamTicker: String,
  optionsStreamExpiration: String,
  optionsStreamStrike: Double,
  optionsStreamType: String,
  optionsStreamFeed: String,
  // Dedicated Redis for options stream
  optionsStreamRedisHost: String,
  optionsStreamRedisPort: Int,
  optionsStreamRedisPassword: String
)

object AppConfig:
  def fromEnv(): AppConfig =
    val redisHostRaw = sys.env.getOrElse(
      "REDIS_HOST",
      "redis-17054.c99.us-east-1-4.ec2.cloud.redislabs.com:17054"
    )
    val (host, port) = redisHostRaw.split(":") match
      case Array(h, p) => (h, p.toIntOption.getOrElse(17054))
      case Array(h)    => (h, 17054)
      case _           => (redisHostRaw, 17054)

    val streamRedisRaw = sys.env.getOrElse(
      "OPTIONS_STREAM_REDIS_HOST",
      "redis-14697.c52.us-east-1-4.ec2.cloud.redislabs.com:14697"
    )
    val (streamHost, streamPort) = streamRedisRaw.split(":") match
      case Array(h, p) => (h, p.toIntOption.getOrElse(14697))
      case Array(h)    => (h, 14697)
      case _           => (streamRedisRaw, 14697)

    AppConfig(
      alpacaApiKey = sys.env.getOrElse("ALPACA_API_KEY", ""),
      alpacaSecretKey = sys.env.getOrElse("ALPACA_SECRET_KEY", ""),
      redisHost = host,
      redisPort = port,
      redisPassword = sys.env.getOrElse("REDIS_PASSWORD", ""),
      pollIntervalMs = sys.env.getOrElse("POLL_INTERVAL_MS", "3000").toLong,
      symbols = sys.env.getOrElse("SYMBOLS", "BTC").split(",").map(_.trim).toSeq,
      cryptoSymbols = sys.env.getOrElse("CRYPTO_SYMBOLS", "BTC/USD").split(",").map(_.trim).toSeq,
      historyMaxSize = sys.env.getOrElse("HISTORY_MAX_SIZE", "10000").toInt,
      optionsSymbols = sys.env.getOrElse("OPTIONS_SYMBOLS", "IWN,CRWD").split(",").map(_.trim).toSeq,
      optionsPollIntervalMs = sys.env.getOrElse("OPTIONS_POLL_INTERVAL_MS", "30000").toLong,
      optionsStreamEnabled = sys.env.getOrElse("OPTIONS_STREAM_ENABLED", "false").toBoolean,
      optionsStreamTicker = sys.env.getOrElse("OPTIONS_STREAM_TICKER", ""),
      optionsStreamExpiration = sys.env.getOrElse("OPTIONS_STREAM_EXPIRATION", ""),
      optionsStreamStrike = sys.env.getOrElse("OPTIONS_STREAM_STRIKE", "0").toDouble,
      optionsStreamType = sys.env.getOrElse("OPTIONS_STREAM_TYPE", "C"),
      optionsStreamFeed = sys.env.getOrElse("OPTIONS_STREAM_FEED", "indicative"),
      optionsStreamRedisHost = streamHost,
      optionsStreamRedisPort = streamPort,
      optionsStreamRedisPassword = sys.env.getOrElse("OPTIONS_STREAM_REDIS_PASSWORD", "")
    )

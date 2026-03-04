package com.helsinki.marketdata.stream

import io.circe.*
import io.circe.syntax.*

case class OptionTradeEvent(
  symbol: String,
  timestamp: String,
  price: Double,
  size: Int,
  exchange: String,
  condition: String
)

case class OptionQuoteEvent(
  symbol: String,
  timestamp: String,
  bidExchange: String,
  bidPrice: Double,
  bidSize: Int,
  askExchange: String,
  askPrice: Double,
  askSize: Int,
  condition: String
)

object OptionsStreamEncoders:
  given Encoder[OptionTradeEvent] = Encoder.instance { t =>
    Json.obj(
      "symbol"    -> Json.fromString(t.symbol),
      "timestamp" -> Json.fromString(t.timestamp),
      "price"     -> Json.fromDoubleOrNull(t.price),
      "size"      -> Json.fromInt(t.size),
      "exchange"  -> Json.fromString(t.exchange),
      "condition" -> Json.fromString(t.condition)
    )
  }

  given Encoder[OptionQuoteEvent] = Encoder.instance { q =>
    Json.obj(
      "symbol"       -> Json.fromString(q.symbol),
      "timestamp"    -> Json.fromString(q.timestamp),
      "bid_exchange" -> Json.fromString(q.bidExchange),
      "bid_price"    -> Json.fromDoubleOrNull(q.bidPrice),
      "bid_size"     -> Json.fromInt(q.bidSize),
      "ask_exchange" -> Json.fromString(q.askExchange),
      "ask_price"    -> Json.fromDoubleOrNull(q.askPrice),
      "ask_size"     -> Json.fromInt(q.askSize),
      "condition"    -> Json.fromString(q.condition)
    )
  }

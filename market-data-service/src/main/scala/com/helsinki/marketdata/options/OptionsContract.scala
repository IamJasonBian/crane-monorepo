package com.helsinki.marketdata.options

import io.circe.*
import io.circe.syntax.*

/** A single options record. Contract, Position, and Order are the same type
  * at different lifecycle stages. Schema matches proto/options_contract.proto.
  *
  * As a quote:    pricing has bid/ask/mid, greeks populated
  * As an order:   pricing has limit_price/stop_price, side/orderType/status set
  * As a position: pricing has avgEntry, sizing.qty and pnl populated
  * All three can coexist on the same record in Redis.
  */
case class OptionsRecord(
  // Identity
  symbol: String,
  underlying: String,
  expiration: String,
  strike: Double,
  optionType: String,             // C or P

  // Sub-attributes
  pricing: Pricing = Pricing(),
  greeks: Greeks = Greeks(),
  sizing: Sizing = Sizing(),
  pnl: PnL = PnL(),

  // Order fields (when this record represents an order)
  side: Option[String] = None,        // buy / sell
  orderType: Option[String] = None,   // limit / stop / stop_limit / market
  status: Option[String] = None,      // open / filled / partial / cancelled
  orderId: Option[String] = None,

  // Multiple orders against this contract
  orders: Seq[OrderEntry] = Seq.empty,

  // History
  bars: Seq[ContractBar] = Seq.empty,

  // Meta
  updatedAt: String = "",
  createdAt: String = ""
)

// All pricing in one place — market, limit, entry
case class Pricing(
  bid: Option[Double] = None,
  ask: Option[Double] = None,
  mid: Option[Double] = None,
  spread: Option[Double] = None,
  lastPrice: Option[Double] = None,
  limitPrice: Option[Double] = None,   // order limit
  stopPrice: Option[Double] = None,    // order stop
  avgEntry: Option[Double] = None      // position entry
)

// Greeks + IV grouped together
case class Greeks(
  delta: Option[Double] = None,
  gamma: Option[Double] = None,
  theta: Option[Double] = None,
  vega: Option[Double] = None,
  rho: Option[Double] = None,
  iv: Option[Double] = None
)

// Quantities — shares, volume, open interest
case class Sizing(
  qty: Option[Int] = None,             // position qty (positive=long, negative=short)
  filledQty: Option[Int] = None,       // order filled qty
  volume: Option[Long] = None,         // market volume
  openInterest: Option[Long] = None    // market OI
)

// Profit & loss
case class PnL(
  unrealizedPl: Option[Double] = None,
  unrealizedPlPct: Option[Double] = None,
  marketValue: Option[Double] = None
)

// An individual order entry (for the repeated orders field)
case class OrderEntry(
  id: String,
  side: String,                        // buy / sell
  orderType: String,                   // limit / stop / stop_limit / market
  limitPrice: Option[Double] = None,
  stopPrice: Option[Double] = None,
  qty: Int,
  status: String,                      // open / filled / partial / cancelled
  filledQty: Option[Int] = None,
  filledAvgPrice: Option[Double] = None,
  createdAt: String = ""
)

case class ContractBar(
  timestamp: String,
  open: Double,
  high: Double,
  low: Double,
  close: Double,
  volume: Long
)


object OptionsRecordCodec:
  private def optD(v: Option[Double]): Json = v.fold(Json.Null)(Json.fromDoubleOrNull)
  private def optI(v: Option[Int]): Json = v.fold(Json.Null)(Json.fromInt)
  private def optL(v: Option[Long]): Json = v.fold(Json.Null)(Json.fromLong)
  private def optS(v: Option[String]): Json = v.fold(Json.Null)(Json.fromString)

  // --- Encoders ---

  given Encoder[Pricing] = Encoder.instance { p =>
    Json.obj(
      "bid" -> optD(p.bid), "ask" -> optD(p.ask), "mid" -> optD(p.mid),
      "spread" -> optD(p.spread), "last_price" -> optD(p.lastPrice),
      "limit_price" -> optD(p.limitPrice), "stop_price" -> optD(p.stopPrice),
      "avg_entry" -> optD(p.avgEntry)
    )
  }

  given Encoder[Greeks] = Encoder.instance { g =>
    Json.obj(
      "delta" -> optD(g.delta), "gamma" -> optD(g.gamma), "theta" -> optD(g.theta),
      "vega" -> optD(g.vega), "rho" -> optD(g.rho), "iv" -> optD(g.iv)
    )
  }

  given Encoder[Sizing] = Encoder.instance { s =>
    Json.obj(
      "qty" -> optI(s.qty), "filled_qty" -> optI(s.filledQty),
      "volume" -> optL(s.volume), "open_interest" -> optL(s.openInterest)
    )
  }

  given Encoder[PnL] = Encoder.instance { p =>
    Json.obj(
      "unrealized_pl" -> optD(p.unrealizedPl),
      "unrealized_pl_pct" -> optD(p.unrealizedPlPct),
      "market_value" -> optD(p.marketValue)
    )
  }

  given Encoder[OrderEntry] = Encoder.instance { o =>
    Json.obj(
      "id" -> Json.fromString(o.id), "side" -> Json.fromString(o.side),
      "order_type" -> Json.fromString(o.orderType),
      "limit_price" -> optD(o.limitPrice), "stop_price" -> optD(o.stopPrice),
      "qty" -> Json.fromInt(o.qty), "status" -> Json.fromString(o.status),
      "filled_qty" -> optI(o.filledQty), "filled_avg_price" -> optD(o.filledAvgPrice),
      "created_at" -> Json.fromString(o.createdAt)
    )
  }

  given Encoder[ContractBar] = Encoder.instance { b =>
    Json.obj(
      "timestamp" -> Json.fromString(b.timestamp),
      "open" -> Json.fromDoubleOrNull(b.open), "high" -> Json.fromDoubleOrNull(b.high),
      "low" -> Json.fromDoubleOrNull(b.low), "close" -> Json.fromDoubleOrNull(b.close),
      "volume" -> Json.fromLong(b.volume)
    )
  }

  given Encoder[OptionsRecord] = Encoder.instance { r =>
    Json.obj(
      "symbol" -> Json.fromString(r.symbol), "underlying" -> Json.fromString(r.underlying),
      "expiration" -> Json.fromString(r.expiration), "strike" -> Json.fromDoubleOrNull(r.strike),
      "option_type" -> Json.fromString(r.optionType),
      "pricing" -> r.pricing.asJson, "greeks" -> r.greeks.asJson,
      "sizing" -> r.sizing.asJson, "pnl" -> r.pnl.asJson,
      "side" -> optS(r.side), "order_type" -> optS(r.orderType),
      "status" -> optS(r.status), "order_id" -> optS(r.orderId),
      "orders" -> Json.fromValues(r.orders.map(_.asJson)),
      "bars" -> Json.fromValues(r.bars.map(_.asJson)),
      "updated_at" -> Json.fromString(r.updatedAt),
      "created_at" -> Json.fromString(r.createdAt)
    )
  }

  // --- Decoders ---

  given Decoder[Pricing] = Decoder.instance { c =>
    Right(Pricing(
      bid = c.get[Double]("bid").toOption, ask = c.get[Double]("ask").toOption,
      mid = c.get[Double]("mid").toOption, spread = c.get[Double]("spread").toOption,
      lastPrice = c.get[Double]("last_price").toOption,
      limitPrice = c.get[Double]("limit_price").toOption,
      stopPrice = c.get[Double]("stop_price").toOption,
      avgEntry = c.get[Double]("avg_entry").toOption
    ))
  }

  given Decoder[Greeks] = Decoder.instance { c =>
    Right(Greeks(
      delta = c.get[Double]("delta").toOption, gamma = c.get[Double]("gamma").toOption,
      theta = c.get[Double]("theta").toOption, vega = c.get[Double]("vega").toOption,
      rho = c.get[Double]("rho").toOption, iv = c.get[Double]("iv").toOption
    ))
  }

  given Decoder[Sizing] = Decoder.instance { c =>
    Right(Sizing(
      qty = c.get[Int]("qty").toOption, filledQty = c.get[Int]("filled_qty").toOption,
      volume = c.get[Long]("volume").toOption, openInterest = c.get[Long]("open_interest").toOption
    ))
  }

  given Decoder[PnL] = Decoder.instance { c =>
    Right(PnL(
      unrealizedPl = c.get[Double]("unrealized_pl").toOption,
      unrealizedPlPct = c.get[Double]("unrealized_pl_pct").toOption,
      marketValue = c.get[Double]("market_value").toOption
    ))
  }

  given Decoder[OrderEntry] = Decoder.instance { c =>
    for
      id        <- c.get[String]("id")
      side      <- c.get[String]("side")
      orderType <- c.get[String]("order_type")
      qty       <- c.get[Int]("qty")
      status    <- c.get[String]("status")
      createdAt <- c.getOrElse[String]("created_at")("")
    yield OrderEntry(
      id = id, side = side, orderType = orderType, qty = qty, status = status, createdAt = createdAt,
      limitPrice = c.get[Double]("limit_price").toOption,
      stopPrice = c.get[Double]("stop_price").toOption,
      filledQty = c.get[Int]("filled_qty").toOption,
      filledAvgPrice = c.get[Double]("filled_avg_price").toOption
    )
  }

  given Decoder[ContractBar] = Decoder.instance { c =>
    for
      ts <- c.get[String]("timestamp"); o <- c.get[Double]("open")
      h <- c.get[Double]("high"); l <- c.get[Double]("low")
      cl <- c.get[Double]("close"); v <- c.get[Long]("volume")
    yield ContractBar(ts, o, h, l, cl, v)
  }

  given Decoder[OptionsRecord] = Decoder.instance { c =>
    for
      symbol     <- c.get[String]("symbol")
      underlying <- c.get[String]("underlying")
      expiration <- c.get[String]("expiration")
      strike     <- c.get[Double]("strike")
      optionType <- c.get[String]("option_type")
      pricing    <- c.getOrElse[Pricing]("pricing")(Pricing())
      greeks     <- c.getOrElse[Greeks]("greeks")(Greeks())
      sizing     <- c.getOrElse[Sizing]("sizing")(Sizing())
      pnl        <- c.getOrElse[PnL]("pnl")(PnL())
      orders     <- c.getOrElse[Seq[OrderEntry]]("orders")(Seq.empty)
      bars       <- c.getOrElse[Seq[ContractBar]]("bars")(Seq.empty)
      updatedAt  <- c.getOrElse[String]("updated_at")("")
      createdAt  <- c.getOrElse[String]("created_at")("")
    yield OptionsRecord(
      symbol = symbol, underlying = underlying, expiration = expiration,
      strike = strike, optionType = optionType,
      pricing = pricing, greeks = greeks, sizing = sizing, pnl = pnl,
      side = c.get[String]("side").toOption,
      orderType = c.get[String]("order_type").toOption,
      status = c.get[String]("status").toOption,
      orderId = c.get[String]("order_id").toOption,
      orders = orders, bars = bars, updatedAt = updatedAt, createdAt = createdAt
    )
  }

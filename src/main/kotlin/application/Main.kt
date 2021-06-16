package application

import arrow.core.NonEmptyList
import arrow.core.nonEmptyListOf
import arrow.typeclasses.Semigroup
import com.binance.api.client.BinanceApiCallback
import com.binance.api.client.BinanceApiClientFactory
import com.binance.api.client.domain.event.CandlestickEvent
import com.binance.api.client.domain.market.CandlestickInterval
import domain.candlestick
import domain.sReducePar
import infrastructure.candlestickdb.Amount
import infrastructure.candlestickdb.Candlestick
import infrastructure.candlestickdb.SimpleCandlestickDatabase
import infrastructure.candlestickdb.Volume

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.receiveAsFlow
import java.time.Instant
import java.time.temporal.ChronoUnit

@ExperimentalCoroutinesApi
fun fetchCandleSticks(symbol: String) = callbackFlow {
    val factory = BinanceApiClientFactory.newInstance()
    val client = factory.newWebSocketClient()
    val callback = object : BinanceApiCallback<CandlestickEvent> {
        override fun onResponse(response: CandlestickEvent?) {
            if (response != null) {
                this@callbackFlow.trySend(
                    Candlestick(
                        symbol = symbol,
                        openTime = Instant.ofEpochMilli(response.openTime),
                        open = Amount.of(response.open),
                        closeTime = Instant.ofEpochMilli(response.closeTime),
                        close = Amount.of(response.close),
                        high = Amount.of(response.high),
                        low = Amount.of(response.low),
                        volume = Volume.of(response.volume),
                        numberOfTrades = response.numberOfTrades
                    )
                )
            }
        }

        override fun onFailure(cause: Throwable?) {
            cancel("Failed to receive candlesticks", cause)
        }
    }

    client.onCandlestickEvent(symbol.lowercase(), CandlestickInterval.ONE_MINUTE, callback)
    awaitClose {}
}

@ExperimentalCoroutinesApi
fun main() = runBlocking {
    val symbol = "BNBBTC";
    val db = SimpleCandlestickDatabase("/tmp/testdb")

    launch {
        fetchCandleSticks(symbol)
            .collect {
                println("Candlestick received: $it")
                db.append(it)
            }
    }
    launch {
        val timeStarted = Instant.now().truncatedTo(ChronoUnit.MINUTES)

        while(true) {
            delay(3000)
            val candlesticks = db.query(symbol, timeStarted, Instant.now().truncatedTo(ChronoUnit.MINUTES))
            if (candlesticks.isNotEmpty()) {
                val aggregated = NonEmptyList.fromListUnsafe(candlesticks).sReducePar(Semigroup.candlestick())
                println("Aggregated ${candlesticks.size} candlesticks: $aggregated")
            }
        }
    }
    Unit
}

package infrastructure.candlestickdb

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import java.nio.file.Path
import java.time.Instant
import kotlin.coroutines.CoroutineContext

const val FILE_EXTENSION = "aof";

class SimpleCandlestickDatabase(private val location: String) : CoroutineScope {
    override val coroutineContext: CoroutineContext
        get() = Job() + Dispatchers.Default
    private val symbolToChannel = mutableMapOf<String, Channel<SymbolAppendOnlyFileRequest>>()

    private fun getSymbolChannel(symbol: String): Channel<SymbolAppendOnlyFileRequest> {
        if (symbolToChannel.containsKey(symbol)) {
            return symbolToChannel[symbol]!!
        }

        val channel = Channel<SymbolAppendOnlyFileRequest>(Channel.UNLIMITED)

        symbolAppendOnlyFileWorker(symbol, Path.of(location).resolve("$symbol.$FILE_EXTENSION").toString(), channel)

        symbolToChannel[symbol] = channel

        return channel
    }

    suspend fun append(candlestick: Candlestick) {
        val channel = getSymbolChannel(candlestick.symbol)
        val deferred = CompletableDeferred<Unit>()

        channel.send(AppendRequest(candlestick, deferred))

        return deferred.await()
    }

    suspend fun append(candlesticks: List<Candlestick>) {
        candlesticks.forEach { append(it) }
    }

    suspend fun query(symbol: String, from: Instant, to: Instant): List<Candlestick> {
        val channel = getSymbolChannel(symbol)
        val deferred = CompletableDeferred<List<Candlestick>>()

        channel.send(QueryRangeRequest(from, to, deferred))

        return deferred.await()
    }

    suspend fun shutdown() {
        symbolToChannel.values.map {
            val deferred = CompletableDeferred<Unit>()

            it.send(ShutdownRequest(deferred))

            deferred
        }.awaitAll()
    }
}


sealed class SymbolAppendOnlyFileRequest
class AppendRequest(val candlestick: Candlestick, val deferred: CompletableDeferred<Unit>) :
    SymbolAppendOnlyFileRequest()

class QueryRangeRequest(val from: Instant, val to: Instant, val deferred: CompletableDeferred<List<Candlestick>>) :
    SymbolAppendOnlyFileRequest()

class ShutdownRequest(val deferred: CompletableDeferred<Unit>) : SymbolAppendOnlyFileRequest()

fun CoroutineScope.symbolAppendOnlyFileWorker(
    symbol: String,
    fileLocation: String,
    requestChannel: ReceiveChannel<SymbolAppendOnlyFileRequest>
) = launch {
    val db = SymbolAppendOnlyFile(symbol, fileLocation)

    requestChannel.consumeEach {
        when (it) {
            is AppendRequest -> {
                try {
                    db.append(it.candlestick)
                    it.deferred.complete(Unit)
                } catch (ex: Exception) {
                    it.deferred.completeExceptionally(ex)
                }
            }
            is QueryRangeRequest -> {
                try {
                    val result = db.query(it.from, it.to)

                    it.deferred.complete(result)
                } catch (ex: Exception) {
                    it.deferred.completeExceptionally(ex)
                }
            }
            is ShutdownRequest -> {
                try {
                    db.close()
                    it.deferred.complete(Unit)
                } catch (ex: Exception) {
                    it.deferred.completeExceptionally(ex)
                }
            }
        }
    }
}

package domain

import arrow.core.NonEmptyList
import arrow.typeclasses.Semigroup
import infrastructure.candlestickdb.Candlestick
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope

// Thought it would be fun to model Candlestick as a Semigroup
fun Semigroup.Companion.candlestick(): Semigroup<Candlestick> {
    return object : Semigroup<Candlestick> {
        override fun Candlestick.combine(b: Candlestick): Candlestick {
            return Candlestick(
                symbol = symbol,
                openTime = openTime,
                open = open,
                closeTime = b.closeTime,
                close = b.close,
                high = high.max(b.high),
                low = low.min(b.low),
                volume = volume + b.volume,
                numberOfTrades = numberOfTrades + b.numberOfTrades
            )
        }
    }
}

// Regular, sequential reduce
fun <A> NonEmptyList<A>.sReduce(semigroup: Semigroup<A>): A {
    return semigroup.run { reduce { acc, a -> acc + a } }
}

// Parallel reduce
suspend fun <A> NonEmptyList<A>.sReducePar(semigroup: Semigroup<A>) = coroutineScope {
    // Detect the number of CPU cores on the machine to divide the work among them
    val numberOfCores = Runtime.getRuntime().availableProcessors() * 2
    val elementsPerCore = size / numberOfCores

    // if there are less than 2 elements per core, just do a sequential reduction
    if (elementsPerCore < 2) {
        sReduce(semigroup)
    } else {
        val lastIndex = numberOfCores - 1
        val remainder = size - (elementsPerCore * numberOfCores)
        // Split the list into view chunks that can be used from threads
        val combined = List(numberOfCores) {
            val start = it * elementsPerCore
            val end = start + elementsPerCore + if (it == lastIndex) remainder else 0

            NonEmptyList.fromListUnsafe(subList(start, end))
        }.map {
            // Dispatch everything on the threadpool
            async(Dispatchers.Default) {
                it.sReduce(semigroup)
            }
        }.awaitAll() // Wait for result

        // Combine the Remaining Candlestick sequentially
        NonEmptyList.fromListUnsafe(combined).sReduce(semigroup)
    }
}

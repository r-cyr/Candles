package application

import arrow.core.NonEmptyList
import infrastructure.candlestickdb.Amount
import infrastructure.candlestickdb.Candlestick
import infrastructure.candlestickdb.Volume
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.random.Random

// Quick function to generate candlesticks... I wanted to remove it but thought it
// would help you understand my thought process
fun generateRandomCandleSticks(symbol: String, openTime: Instant, quantity: Int): NonEmptyList<Candlestick> {
    require(quantity > 0) { "Quantity should be greater than 0" }

    fun randomCurrency() = Amount.of(Random.nextDouble().toString())
    fun randomVolume() = Volume.of(Random.nextInt(10, 100).toString())
    fun randomNumberOfTrades() = Random.nextLong(2, 40)

    return NonEmptyList.fromListUnsafe(
        generateSequence(
            Candlestick(
                symbol = symbol,
                openTime = openTime.truncatedTo(ChronoUnit.MINUTES),
                open = randomCurrency(),
                closeTime = openTime.plus(1, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.MINUTES),
                close = randomCurrency(),
                high = randomCurrency(),
                low = randomCurrency(),
                volume = randomVolume(),
                numberOfTrades = randomNumberOfTrades()
            )
        ) {
            Candlestick(
                symbol = it.symbol,
                openTime = it.closeTime.truncatedTo(ChronoUnit.MINUTES),
                open = it.close,
                closeTime = it.closeTime.plus(1, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.MINUTES),
                close = randomCurrency(),
                high = randomCurrency(),
                low = randomCurrency(),
                volume = randomVolume(),
                numberOfTrades = randomNumberOfTrades()
            )
        }.take(quantity).toList()
    )
}

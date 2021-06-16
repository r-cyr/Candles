package infrastructure.candlestickdb

import java.nio.MappedByteBuffer
import java.time.Instant

const val CANDLESTICK_PADDING = 32

const val CANDLESTICK_BYTE_LENGTH = 256

fun putCandlestick(candlestick: Candlestick, mappedByteBuffer: MappedByteBuffer) {
    with(mappedByteBuffer) {
        putLong(candlestick.openTime.toEpochMilli())                       //  8 Bytes
        putBigDecimal(candlestick.open.toBigDecimal(), this)    // 40 Bytes
        putLong(candlestick.closeTime.toEpochMilli())                      //  8 Bytes
        putBigDecimal(candlestick.close.toBigDecimal(), this)   // 40 Bytes
        putBigDecimal(candlestick.high.toBigDecimal(), this)    // 40 Bytes
        putBigDecimal(candlestick.low.toBigDecimal(), this)     // 40 Bytes
        putBigDecimal(candlestick.volume.toBigDecimal(), this)  // 40 Bytes
        putLong(candlestick.numberOfTrades)                             //  8 Bytes
        // Padding
        put(ByteArray(CANDLESTICK_PADDING) { 0 })                                   // 32 Bytes
    }
}

fun getCandlestickForSymbol(symbol: String, mappedByteBuffer: MappedByteBuffer): Candlestick {
    with(mappedByteBuffer) {
        val openTime = Instant.ofEpochMilli(long)
        val open = Amount(getBigDecimal(mappedByteBuffer))
        val closeTime = Instant.ofEpochMilli(long)
        val close = Amount(getBigDecimal(mappedByteBuffer))
        val high = Amount(getBigDecimal(mappedByteBuffer))
        val low = Amount(getBigDecimal(mappedByteBuffer))
        val volume = Volume(getBigDecimal(mappedByteBuffer))
        val numberOfTrades = long
        position(position() + CANDLESTICK_PADDING)

        return Candlestick(
            symbol = symbol,
            openTime = openTime,
            open = open,
            closeTime = closeTime,
            close = close,
            high = high,
            low = low,
            volume = volume,
            numberOfTrades = numberOfTrades
        )
    }
}

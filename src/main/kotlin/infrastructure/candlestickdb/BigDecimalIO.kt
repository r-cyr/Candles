package infrastructure.candlestickdb

import java.math.BigDecimal
import java.math.BigInteger
import java.nio.MappedByteBuffer

const val MAX_BIG_DECIMAL_UNSCALED_PART_LENGTH = 32

class BigDecimalTooLarge(message: String): Exception(message)

// Writes a BigDecimal as fixed length data(40 Bytes):
// 4 Bytes: Size of integer byte buffer
// 32 Bytes: Integer bytes padded with 0s
// 4 Bytes: BigDecimal "scale" value
fun putBigDecimal(bigDecimal: BigDecimal, mappedByteBuffer: MappedByteBuffer) {
    val integerPart = bigDecimal.unscaledValue();
    val integerBytes = integerPart.toByteArray()

    if (integerBytes.size > MAX_BIG_DECIMAL_UNSCALED_PART_LENGTH) {
        throw BigDecimalTooLarge("BigDecimal cannot be stored: ${integerBytes.size} > $MAX_BIG_DECIMAL_UNSCALED_PART_LENGTH")
    }

    val integerBytesPadding = ByteArray(MAX_BIG_DECIMAL_UNSCALED_PART_LENGTH - integerBytes.size) { 0 }
    val scale = bigDecimal.scale()

    with(mappedByteBuffer) {
        putInt(integerBytes.size)
        put(integerBytes)
        put(integerBytesPadding)
        putInt(scale)
    }
}

fun getBigDecimal(mappedByteBuffer: MappedByteBuffer): BigDecimal {
    with(mappedByteBuffer) {
        val integerBytesSize = int
        val integerBytes = ByteArray(integerBytesSize);
        get(integerBytes);
        val paddingToSkip = MAX_BIG_DECIMAL_UNSCALED_PART_LENGTH - integerBytesSize
        position(position() + paddingToSkip)
        val scale = int

        return BigDecimal(BigInteger(integerBytes), scale)
    }
}

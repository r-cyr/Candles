package infrastructure.candlestickdb

import java.math.BigDecimal
import java.time.Instant

@JvmInline
value class Amount(private val value: BigDecimal) {
    companion object {
        fun of(quantity: String): Amount {
            return Amount(BigDecimal(quantity))
        }

        fun of(quantity: BigDecimal): Amount {
            return Amount(quantity)
        }
    }

    fun min(other: Amount): Amount {
        return Amount(value.min(other.value))
    }

    fun max(other: Amount): Amount {
        return Amount(value.max(other.value))
    }

    fun toBigDecimal(): BigDecimal {
        return value
    }
}


@JvmInline
value class Volume(private val value: BigDecimal) {
    companion object {
        fun of(quantity: String): Volume {
            return Volume(BigDecimal(quantity))
        }

        fun of(quantity: BigDecimal): Volume {
            return Volume(quantity)
        }
    }

    operator fun plus(volume: Volume): Volume {
        return Volume(value + volume.value)
    }

    fun toBigDecimal(): BigDecimal {
        return value
    }
}

data class Candlestick(
    val symbol: String,
    val openTime: Instant,
    val open: Amount,
    val closeTime: Instant,
    val close: Amount,
    val high: Amount,
    val low: Amount,
    val volume: Volume,
    val numberOfTrades: Long,
)
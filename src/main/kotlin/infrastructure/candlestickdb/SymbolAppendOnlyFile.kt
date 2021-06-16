package infrastructure.candlestickdb


import java.io.File
import java.io.RandomAccessFile
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.time.Instant
import java.time.temporal.ChronoUnit

const val PAGE_SIZE = 4096L
const val HEADER_LENGTH = 256

class InvalidCandlestickSymbol(message: String) : Exception(message)

class SymbolAppendOnlyFile(private val symbol: String, fileLocation: String) : AutoCloseable {
    private val isNew = !File(fileLocation).isFile
    private val randomAccessFile =
        RandomAccessFile(fileLocation, "rw")
    var size: Long = if (isNew) PAGE_SIZE else randomAccessFile.length()
        private set

    private val fileChannel = randomAccessFile.channel;
    private var mappedByteBuffer =
        fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size)
            .also {
                if (isNew) {
                    writeNewFileHeader(it)
                }
            }
    private var maxEntries = size / CANDLESTICK_BYTE_LENGTH - 1
    private var lastAppendOpenTime = if (isNew) Instant.EPOCH else getOpenTimeAtEntry(getNumberOfEntries().toInt() - 1)

    private fun writeNewFileHeader(mappedByteBuffer: MappedByteBuffer) {
        mappedByteBuffer.position(0)
        mappedByteBuffer.putLong(0)
        mappedByteBuffer.put(ByteArray(248) { 0 })
    }

    private fun allocateMoreStorage() {
        val position = mappedByteBuffer.position()

        unmapHack(fileChannel, mappedByteBuffer)
        size += PAGE_SIZE
        mappedByteBuffer =
            fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size)
        maxEntries = size / CANDLESTICK_BYTE_LENGTH - 1
        mappedByteBuffer.position(position)
    }

    private fun unmapHack(fileChannel: FileChannel, mappedByteBuffer: MappedByteBuffer) {
        val clazz = fileChannel.javaClass
        val unmapMethod = clazz.getDeclaredMethod("unmap", MappedByteBuffer::class.java)

        if (unmapMethod.trySetAccessible()) {
            unmapMethod.invoke(null, mappedByteBuffer)
        }
    }

    private fun getNumberOfEntries(): Long {
        val currentPosition = mappedByteBuffer.position()

        mappedByteBuffer.position(0)
        val numberOfEntries = mappedByteBuffer.long
        mappedByteBuffer.position(currentPosition);

        return numberOfEntries
    }

    private fun setNumberOfEntries(value: Long) {
        mappedByteBuffer.putLong(0, value)
    }

    private fun positionAtEntry(index: Int): Unit {
        mappedByteBuffer.position((index * CANDLESTICK_BYTE_LENGTH) + HEADER_LENGTH)
    }

    private fun getOpenTimeAtEntry(entryIndex: Int): Instant {
        positionAtEntry(entryIndex)
        return Instant.ofEpochMilli(mappedByteBuffer.long)
    }

    // Using a good old binary search because I'm assuming "missing entries"
    // If no missing entries then random access lookups would be possible
    private fun findEntry(instant: Instant, left: Int, right: Int): Int? {
        if (right >= left) {
            val mid = left + (right - left) / 2;
            val midInstant = getOpenTimeAtEntry(mid);

            if (midInstant == instant) {
                return mid
            }

            if (midInstant.isAfter(instant)) {
                return findEntry(instant, left, mid - 1)
            }

            return findEntry(instant, mid + 1, right)
        }

        return null
    }

    fun append(candlestick: Candlestick) {
        if (candlestick.symbol != symbol) {
            throw InvalidCandlestickSymbol("C")
        }

        // If the candlestick's open time is before or equal to the latest
        // that was inserted then we just drop it.
        if (!candlestick.openTime.isAfter(lastAppendOpenTime)) {
            return
        }

        val numberOfEntries = getNumberOfEntries()

        if (numberOfEntries >= maxEntries) {
            allocateMoreStorage()
        }

        positionAtEntry(numberOfEntries.toInt());

        putCandlestick(candlestick, mappedByteBuffer)

        setNumberOfEntries(numberOfEntries + 1)

        lastAppendOpenTime = candlestick.openTime
    }

    fun query(from: Instant, to: Instant): List<Candlestick> {
        val fromEntry = findEntry(from, 0, getNumberOfEntries().toInt() - 1) ?: return listOf()
        val toEntry = findEntry(to, 0, getNumberOfEntries().toInt() - 1) ?: return listOf()

        positionAtEntry(fromEntry)

        return (fromEntry..toEntry).map { getCandlestickForSymbol(symbol, mappedByteBuffer) }
    }

    override fun close() {
        mappedByteBuffer.force()
        fileChannel.close()
        randomAccessFile.close()
    }
}

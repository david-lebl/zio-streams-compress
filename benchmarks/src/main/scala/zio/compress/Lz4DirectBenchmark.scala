package zio.compress

import net.jpountz.lz4.{LZ4Compressor, LZ4Factory, LZ4SafeDecompressor}
import org.openjdk.jmh.annotations._

import java.util.concurrent.TimeUnit

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(2)
class Lz4DirectBenchmark {

  @Param(Array("1024", "65536", "1048576"))
  var dataSize: Int = _

  @Param(Array("compressible", "random"))
  var dataType: String = _

  @Param(Array("fast", "high"))
  var compressorType: String = _

  private val factory                           = LZ4Factory.fastestInstance()
  private val decompressor: LZ4SafeDecompressor = factory.safeDecompressor()
  private var compressor: LZ4Compressor         = _

  private var inputData: Array[Byte]      = _
  private var compressedData: Array[Byte] = _
  private var compressedLen: Int          = _
  private var compressBuf: Array[Byte]    = _
  private var decompressBuf: Array[Byte]  = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    compressor = compressorType match {
      case "fast" => factory.fastCompressor()
      case "high" => factory.highCompressor()
      case other  => throw new IllegalArgumentException(s"Unknown compressorType: $other")
    }

    inputData = dataType match {
      case "compressible" =>
        Array.fill(dataSize)(0x42.toByte)
      case "random" =>
        val rng = new java.util.Random(0xdeadbeefL)
        val arr = new Array[Byte](dataSize)
        rng.nextBytes(arr)
        arr
      case other =>
        throw new IllegalArgumentException(s"Unknown dataType: $other")
    }

    compressBuf = new Array[Byte](compressor.maxCompressedLength(dataSize))

    compressedLen = compressor.compress(inputData, 0, dataSize, compressBuf, 0)
    compressedData = java.util.Arrays.copyOf(compressBuf, compressedLen)

    decompressBuf = new Array[Byte](dataSize)
  }

  @Benchmark
  def compress(): Int =
    compressor.compress(inputData, 0, dataSize, compressBuf, 0)

  @Benchmark
  def decompress(): Int =
    decompressor.decompress(compressedData, 0, compressedLen, decompressBuf, 0, dataSize)
}

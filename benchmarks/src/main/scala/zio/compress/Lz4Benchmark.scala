package zio.compress

import org.openjdk.jmh.annotations._
import zio.{Chunk, Runtime, Task, Unsafe}
import zio.stream.ZStream

import java.util.concurrent.TimeUnit

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(2)
class Lz4Benchmark {

  @Param(Array("1024", "65536", "1048576"))
  var dataSize: Int = _

  @Param(Array("compressible", "random"))
  var dataType: String = _

  @Param(Array("4096", "65536", "262144"))
  var chunkSize: Int = _

  private var inputData: Chunk[Byte]      = _
  private var compressedData: Chunk[Byte] = _
  private val runtime: Runtime[Any]       = Runtime.default

  @Setup(Level.Trial)
  def setup(): Unit = {
    inputData = dataType match {
      case "compressible" =>
        Chunk.fill(dataSize)(0x42.toByte)
      case "random" =>
        val rng   = new java.util.Random(0xdeadbeefL)
        val array = new Array[Byte](dataSize)
        rng.nextBytes(array)
        Chunk.fromArray(array)
      case other =>
        throw new IllegalArgumentException(s"Unknown dataType: $other")
    }
    compressedData = runZIO(
      ZStream.fromChunk(inputData).via(Lz4Compressor.compress).runCollect
    )
  }

  @Benchmark
  def compress(): Chunk[Byte] =
    runZIO(ZStream.fromChunk(inputData).via(Lz4Compressor.compress).runCollect)

  @Benchmark
  def decompress(): Chunk[Byte] =
    runZIO(ZStream.fromChunk(compressedData).via(Lz4Decompressor(chunkSize).decompress).runCollect)

  @Benchmark
  def roundTrip(): Chunk[Byte] =
    runZIO(
      ZStream
        .fromChunk(inputData)
        .via(Lz4Compressor.compress)
        .via(Lz4Decompressor(chunkSize).decompress)
        .runCollect
    )

  private def runZIO[A](effect: Task[A]): A =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe.run(effect).getOrThrow()
    }
}

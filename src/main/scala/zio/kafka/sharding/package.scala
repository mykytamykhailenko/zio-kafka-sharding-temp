package zio.kafka

import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream, Decoder, Encoder}
import zio.kafka.consumer.Offset
import zio.kafka.sharding.PartitionRegisterer.{ShardEvent, shardEventEncoder}
import zio.{Chunk, Task, ZIO}

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import scala.collection.{immutable, mutable}

package object sharding {

  def groupBy[A, K](collection: Iterable[A])(key: A => K): Map[K, Vector[A]] = {

    // This method does not leak any mutable code.
    val grouped: mutable.Map[K, immutable.Queue[A]] = mutable.Map()

    collection.foldLeft(grouped) { (grouped, item) =>

      grouped.updateWith(key(item))(_.map(_.appended(item)).orElse(Some(immutable.Queue(item))))

      grouped
    }

    grouped.view.mapValues(_.toVector).toMap
  }

  type Done = Unit

  type Partition = Int

  type ByteArray = Array[Byte]

  type ID = String

  case class Event[T](id: ID,
                      event: T,
                      reply: Boolean)

  case class EventBatch[T](events: Chunk[Event[T]], offset: Offset)

  sealed trait Failure

  case object Timeout extends Failure

  case object Rebalance extends Failure

  case object Idle extends Failure

  case object ShardGroupNotFound extends Failure

  case object SameShardRegisteredTwice extends Failure

  case object AmbiguousEntityCreationPolicy extends Failure

  def deserialize[T](decoder: Decoder[T])(eventBytes: ByteArray): Task[T] =
    ZIO.attempt {

      val outputStream =
        AvroInputStream
          .binary[T](decoder)
          .from(eventBytes)
          .build(decoder.schema)

      val event = outputStream.iterator.next()

      outputStream.close()

      event
    }

  def serialize[T](encoder: Encoder[T])(event: T): Task[ByteArray] =
    ZIO.attempt {

      val binaryStream = new ByteArrayOutputStream()

      val stream =
        AvroOutputStream
          .binary[T](encoder)
          .to(binaryStream)
          .build

      stream.write(event)
      stream.flush()
      stream.close()

      binaryStream.toByteArray
    }

  def deserializeShardEvent[T](decoder: Decoder[T])(shardEvent: ShardEvent): Task[Event[T]] = {
    val ShardEvent(_, id, eventBytes, reply) = shardEvent

    deserialize(decoder)(eventBytes).map(Event(id, _, reply))
  }

  // Preferably, there must be another DTO, which does not store the ID.
  // Be careful! I use binary serialization, so the schema is required!
  def serializeShardEvent[T](encoder: Encoder[T])(entityID: ID, id: ID, event: T, reply: Boolean): Task[ByteArray] =
  for {
    shardEvent <- serialize(encoder)(event).map(ShardEvent(entityID, id, _, reply))
    bytes <- serialize(shardEventEncoder)(shardEvent)
  } yield bytes




}

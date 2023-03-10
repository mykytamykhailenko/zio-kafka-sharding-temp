package zio.kafka.sharding

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import zio._
import zio.kafka.sharding.Entity._
import zio.kafka.sharding.PartitionRegisterer.{ShardEventStream, _}
import zio.kafka.sharding.Sharding.ShardingDescription
import zio.kafka.sharding.model.{EntityDescription, Shard, ShardGroup}
import zio.stream._

private[sharding]
case class PartitionRegisterer[T](entityDescription: EntityDescription[T],
                                  shardingDescription: ShardingDescription,
                                  sharding: Sharding) {

  val shardingLayer: ULayer[Sharding] = ZLayer.succeed(sharding)

  val EntityDescription(typeTag, entityID, behavior, terminate, encoder, decoder, idleTimeout, terminateTimeout) = entityDescription

  val ShardingDescription(backpressure, chunkSize, unmarshalFib) = shardingDescription

  /**
   * Creates an entity and inserts it into the entity map.
   * Besides, it enqueues all events to the entity's message box
   * and asks to process all of them.
   *
   * @param shard  Shard
   * @param id     Entity ID
   * @param events Entity events
   * @return Effect
   */
  private def handleEvents(shard: Shard[T],
                           id: ID,
                           events: Vector[T],
                           createIfNotExists: Boolean,
                           shutdownIdle: Boolean): URIO[Sharding, Done] = {

    import shard._


    val createUnit = for {
      trigger <- ZIO.when(shutdownIdle)(createTrigger(expired, id, idleTimeout))
        .someOrElseZIO(ZIO.succeed(ZIO.unit))
      entity <- createEntity(id, behavior, trigger, terminate)

      /*
      There is a window between 'entities.get' and 'entities.putIfAbsent'
      during which another entity could be created.

      The entity will never be created during this time window because for that:
      1. 'handleEvents' would be executed in parallel
      2. It must be executed for the same entity ID

      The second condition will never happen,
      because all events are grouped by the entity ID,
      meaning 'handleEvents' will never be execute in parallel
      for the same entity.

      I have decided to keep this code just be on the safe side.
      */
      occupied <- entities.putIfAbsent(id, entity)
      _ <- occupied.fold(ZIO.unit)(_.shutdown(backpressure))
    } yield occupied.getOrElse(entity)

    val handled = for {
      entity <- entities.get(id)

      entity <-
        ZIO.fromOption(entity)
          .asSome
          .catchAll(_ => ZIO.when(createIfNotExists)(createUnit))

      entity <- ZIO.fromOption(entity)
      _ <- entity.offerAll(events)
      _ <- entity.receiveN(events.size, backpressure)
    } yield ()

    handled.ignore
  }

  private def handleChunk(shard: Shard[T],
                          events: Map[ID, Vector[Event[T]]]): ZIO[Sharding, Failure, Done] = {


    val digestEvents = ZIO.foreachParDiscard(events) { case (id, events) =>

      val (replies, questions) = events.partitionMap { case Event(_, event, reply) =>
        if (reply) Left(event)
        else Right(event)
      }

      if (replies.nonEmpty && questions.isEmpty)
        handleEvents(shard, id, replies,
          createIfNotExists = false,
          shutdownIdle = false)

      else if (questions.nonEmpty && replies.isEmpty)
        handleEvents(shard, id, questions,
          createIfNotExists = true,
          shutdownIdle = true)

      else ZIO.fail(AmbiguousEntityCreationPolicy)
    }

    shard.shutdownIdleEntities(backpressure, terminateTimeout) *> digestEvents
  }

  def createShardGroup: UIO[ShardGroup[T]] = sharding.createShardGroup[T](entityDescription)

  /**
   * You can add a single partition stream.
   *
   * You must ensure the stream contains events belonging to the same partition,
   * otherwise you may encounter unpredictable behavior.
   *
   * @param partition The partition
   * @param stream    The event stream
   */
  def registerPartition(partition: Partition, stream: ShardEventStream): IO[Failure, Fiber.Runtime[Failure, Done]] =

    for {
      shard <- Shard.make[T]

      shardGroup <- sharding.getShardGroup[T](typeTag)

      _ <- shardGroup.put(partition, shard)

      streaming =
        stream
          .mapZIOPar(unmarshalFib)(deserializeShardEvent(decoder))
          .orDie
          .bufferChunks(chunkSize)
          .runForeachChunk { chunk =>

            val events = groupBy(chunk)(_.id)

            handleChunk(shard, events)
          }

      _ <- ZIO.logInfo(s"[PartitionRegister-$entityID] has registered partition number $partition")

      fib <-
        streaming
          .ensuring(ZIO.logInfo(s"[PartitionRegister-$entityID] partition number $partition has been unregistered"))
          .ensuring(shard
            .shutdown(backpressure, terminateTimeout)
            .tapDefect(defect => ZIO.logCause(defect))
            .ignore)
          .ensuring(shardGroup.remove(partition))
          .provide(shardingLayer)
          .tapDefect(defect => ZIO.logCause(defect))
          .fork

    } yield fib

  def registerPartitionAwait(partition: Partition, stream: ShardEventStream): IO[Failure, Done] =
    registerPartition(partition, stream).flatMap(_.await).unit


}

object PartitionRegisterer {

  case class ShardEvent(entityID: ID,
                        id: ID,
                        eventBytes: ByteArray,
                        reply: Boolean)

  private val schemaForShardEvent: SchemaFor[ShardEvent] = SchemaFor[ShardEvent]

  val shardEventDecoder: Decoder[ShardEvent] = Decoder[ShardEvent].withSchema(schemaForShardEvent)

  val shardEventEncoder: Encoder[ShardEvent] = Encoder[ShardEvent].withSchema(schemaForShardEvent)

  type ShardEventStream = Stream[Throwable, ShardEvent]


  /**
   * Creates a ''trigger''.
   *
   * The trigger is the reset button of the stopwatch.
   * Each time you click the reset button, the timer start over.
   * Once the stopwatch reaches the idle timeout, the entity is
   * considered to be inactive.
   * Then, it will be added to the ''expired'' queue,
   * and will be descheduled eventually.
   *
   * @param expired The expiration queue
   * @param id      The entity ID
   * @return The ''trigger''
   */
  private[sharding] def createTrigger(expired: Queue[ID], id: ID, idleTimeout: Duration): UIO[UIO[Done]] =
    for {
      queue <- Queue.bounded[Done](1)

      promise <- Promise.make[Nothing, Done]

      enqueue = promise.await *> queue.shutdown *> expired.offer(id)

      _ <- enqueue.fork

      complete = ZIO.sleep(idleTimeout) *> promise.succeed()

      _ <- complete.fork.flatMap(fiber => queue.take *> fiber.interrupt).forever.fork

      // Forking this effect prevents interruptions from poisoning code.
    } yield queue.offer().unit.fork.unit


}

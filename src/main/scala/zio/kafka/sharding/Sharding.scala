package zio.kafka.sharding

import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import zio._
import zio.kafka.consumer.Consumer
import zio.kafka.producer.Producer
import zio.kafka.serde.{Deserializer, Serializer}
import zio.kafka.sharding.Entity.{ID, createEntity}
import zio.kafka.sharding.PartitionRegisterer.shardEventDecoder
import zio.kafka.sharding.Sharding.{ByteArrayPartitionedStream, ShardingDescription}
import zio.kafka.sharding.model.{EntityDescription, PartitionRegisterPool, ShardGroup}
import zio.stream._

import scala.reflect.runtime.universe._

/**
 * ''Sharding'' is responsible for
 *
 * @param shardingDescription
 * @param shardGroups
 * @param semaphore
 * @param consumer
 * @param producer
 * @param topic
 */
case class Sharding private[sharding](private val shardingDescription: ShardingDescription,
                                      private val shardGroups: Ref[Seq[ShardGroup[_]]],
                                      private val semaphore: Semaphore,
                                      private val consumer: Consumer,
                                      private val producer: Producer,
                                      private val topic: String) {
  self =>

  private val ShardingDescription(backpressure, chunkSize, unmarshalFib) = shardingDescription

  private val shardingLayer: ULayer[Sharding] = ZLayer.succeed(self)

  /**
   * You can find the shard group by its type.
   *
   * You can encounter ''ShardGroupNotFound'' if the shard group was not registered before.
   *
   * @param typeTag The type tag
   * @tparam T The type
   * @return
   */
  private[sharding] def getShardGroup[T](implicit typeTag: TypeTag[T]): IO[Failure, ShardGroup[T]] =
    semaphore.withPermit {
      for {
        shardGroup <- shardGroups.get

        group =
          shardGroup
            .find(_.entityDescription.typeTag.tpe =:= typeTag.tpe)
            .map(_.asInstanceOf[ShardGroup[T]])

        group <- ZIO.fromOption(group).catchAll(_ => ZIO.fail(ShardGroupNotFound))

      } yield group
    }

  /**
   * Creates a shard group based on the entity description.
   * The shard group needs the entity description to access its type tag for reflective access.
   *
   * @param entityDescription The entity description
   * @tparam T The entity type
   * @return The shard group
   */
  private[sharding] def createShardGroup[T](entityDescription: EntityDescription[T]): UIO[ShardGroup[T]] =
    semaphore.withPermit {
      for {
        shardGroup <- ShardGroup.make[T](entityDescription)
        _ <- shardGroups.update(shardGroup +: _)
      } yield shardGroup
    }

  /**
   * You can access the ''Shard'' instance by specifying the ''TypeTag''.
   * The ''Shard'' lets you send events over a Kafka topic.
   * You can ask for a single reply or a stream of replies from other entities using the ''Shard'' instance.
   *
   * @param typeTag The type tag
   * @tparam T The entity type
   * @return The ''Shard''
   */
  def getShard[T](implicit typeTag: TypeTag[T]): IO[Failure, Sharding.Shard[T]] =
    getShardGroup[T](typeTag).map(Sharding.Shard(shardingLayer, _, topic, producer))

  /**
   * You can access the ''Shard'' instance by providing the entity description.
   * It is another version of ''getShard'', which yields better performance,
   * because it does not need to access the type tag reflectively.
   *
   * @param entityDescription The entity description
   * @tparam T The entity type
   * @return The ''Shard''
   */
  def getShard[T](entityDescription: EntityDescription[T]): IO[Failure, Sharding.Shard[T]] =
    getShard(entityDescription.typeTag)

  private[sharding] def startSharding(stream: ByteArrayPartitionedStream,
                                      partitionRegisterPool: PartitionRegisterPool): UIO[Sharding] = {

    val sharding =
      stream
        .orDie
        .runForeach { case (partition, partitionStream) =>

          ZIO.logInfo(s"[Sharding] partition $partition has been assigned to this pod") *>
            partitionStream
              .mapZIOPar(unmarshalFib)(deserialize(shardEventDecoder))
              .orDie
              .groupByKey(_.entityID, chunkSize)(partitionRegisterPool(partition), chunkSize)
              .runDrain
              .ensuring(ZIO.logInfo(s"[Sharding] partition $partition has been descheduled from this pod"))
              .tapDefect(defect => ZIO.logCause(defect))
              .fork
        }

    partitionRegisterPool.registerShardGroups *>
      sharding
        .tapDefect(defect => ZIO.logCause(defect))
        .ignore
        .fork *> ZIO.succeed(self)
  }

  private[sharding] def start(entityDescriptions: EntityDescription[_]*): UIO[Sharding] = {

    val stream: ByteArrayPartitionedStream =
      consumer
        .partitionedStream(Deserializer.string, Deserializer.byteArray)
        .map { case (topicPartition, stream) => topicPartition.partition() -> stream.map(_.value) }

    startSharding(stream, PartitionRegisterPool(self, shardingDescription, entityDescriptions))
  }

}

object Sharding {

  type ByteArrayPartitionedStream = Stream[Throwable, (Partition, Stream[Throwable, ByteArray])]

  case class ShardingDescription(backpressure: Boolean = true,
                                 chunkSize: Int = 2048,
                                 unmarshalFib: Int = 8)

  /**
   *
   * @param shardingDescription
   * @param consumer
   * @param producer
   * @param entityDescriptions
   * @return
   */
  def make(shardingDescription: ShardingDescription,
           consumer: Consumer,
           producer: Producer,
           entityDescriptions: EntityDescription[_]*): UIO[Sharding] = for {

    topic <- consumer.subscription.orDie
    semaphore <- Semaphore.make(1)
    shardGroups <- Ref.make(Seq.empty[ShardGroup[_]])
    sharding <- Sharding(shardingDescription, shardGroups, semaphore, consumer, producer, topic.head).start(entityDescriptions: _*)

  } yield sharding

  case class Reply[T] private[sharding](private val replyId: ID,
                                        private val replyPartition: Partition) {

    def reply(event: T)(implicit tag: Tag[T]): RIO[Sharding.Shard[T], UIO[Done]] =
      ZIO.serviceWithZIO[Sharding.Shard[T]](_.tell(replyId, replyPartition, event))

  }

  private val idSize = 10

  case class Shard[T] private[sharding](private val shardingLayer: ULayer[Sharding],
                                        private val shardGroup: ShardGroup[T],
                                        private val topic: String,
                                        private val producer: Producer,
                                        /*private val hub: Hub[Partition]*/) {

    private def ignoreInterruption[R, E, A](zio: ZIO[R, E, A]): ZIO[R, E, Done] =
      zio
        .unit
        .catchAllCause(cause =>
          if (cause.isInterruptedOnly) ZIO.unit
          else ZIO.failCause(cause))


    private def ignoreInterruptionFork[R, E, A](zio: ZIO[R, E, A]): URIO[R, Fiber.Runtime[E, Done]] =
      ignoreInterruption(zio)
        .tapDefect(defect => ZIO.logCause(defect))
        .fork


    /**
     * Create an entity waiting for reply.
     * The entity will automatically terminate after specified timeout.
     * Then, the entity won't respond to any events and become idle.
     * The underlying promise will fail with ''Timeout''.
     *
     * @param timeout Timeout
     * @return The entity and the promise
     */
    private def replyOnceEntity(rebalance: Promise[Nothing, Done], timeout: Duration): UIO[(Promise[Failure, T], Entity[T])] =
      for {
        promise <- Promise.make[Failure, T]

        timeout <- ignoreInterruptionFork(ZIO.sleep(timeout) *> promise.fail(Timeout))

        // This is a long-running fiber, which may never get suspended.
        // Thus, it will never be garbage collected.
        // This code overcomes this challenge.
        resharding <- ignoreInterruptionFork(rebalance.await *> promise.fail(Rebalance))

        replyBehavior = (_: ID) =>
          ZIO.succeed((event: T) =>
            ZIO.succeed(promise.succeed(event) *>
              ZIO.interrupt))

        replyId <- Random.nextString(idSize)

        entity <- createEntity[T](replyId, replyBehavior).provide(shardingLayer)

        _ <-
          promise
            .await
            .ignore
            .ensuring(resharding.interrupt *> timeout.interrupt)
            .tapDefect(defect => ZIO.logCause(defect))
            .fork

      } yield (promise, entity)

    /**
     * Create an entity waiting for reply.
     * The entity will cease waiting when ''timeout'' elapses.
     * It will terminate itself and fail underlying promise with ''Timeout''.
     *
     * The entity won't be cleaned up,
     * but it won't respond to any events.
     *
     * @param id        The entity's ID
     * @param replyOnce The function
     * @param timeout   Timeout
     * @return
     */
    def replyOnce(id: ID,
                  replyOnce: Reply[T] => T,
                  timeout: Duration = 30.seconds): UIO[Promise[Failure, T]] = {

      for {
        (partition, shard) <- shardGroup.getShardRoundRobin

        (promise, entity) <- replyOnceEntity(shard.rebalance, timeout)

        replyId = entity.id

        _ <- shard.entities.put(replyId, entity)

        _ <-
          promise
            .await
            .ignore
            .ensuring(entity.shutdown(backpressure = true))
            .ensuring(shard.entities.remove(replyId))
            .fork

        embedded = replyOnce(Reply(replyId, partition))

        _ <- tell(id, embedded).flatten

      } yield promise
    }

    /**
     * Creates a stream containing replies.
     * Once the requester doesn't need the stream any more,
     * he can close the stream via ''haltWhen'' or ''interruptWhen''.
     *
     * The underlying entity will be automatically terminated once
     * the stream is closed.
     *
     * The stream will be automatically interrupted after rebalance.
     *
     * @param id          The entity's ID
     * @param reply       The function
     * @param idleTimeout idle timeout
     * @return The stream
     */
    def reply(id: ID,
              reply: Reply[T] => T,
              idleTimeout: Option[Duration] = Some(5.minutes)): UIO[Stream[Failure, T]] =

      for {
        (partition, shard) <- shardGroup.getShardRoundRobin

        queue <- Queue.unbounded[T]

        replyBehavior = (_: ID) =>
          ZIO.succeed((event: T) =>
            ZIO.succeed(queue.offer(event).unit)) // This code may interrupt, but it is OK.

        replyId <- Random.nextString(idSize)

        entity <- createEntity[T](replyId, replyBehavior).provide(shardingLayer)

        _ <- shard.entities.put(replyId, entity)

        embedded = reply(Reply(replyId, partition))

        partition <- tell(id, embedded).flatten
/*
        promise <- Promise.make[Nothing, Done]

        _ <- ZIO.scoped(
          ZStream
            .fromHub(hub)
            .filter(_ == partition)
            .runForeach(_ => promise.succeed().unit)).fork*/

        stream =
          ZStream
            .fromQueueWithShutdown(queue)
            .interruptWhen(shard.rebalance.await)
            //.interruptWhen(promise.await)
            .ensuring(shard.entities.remove(replyId))

      } yield idleTimeout.fold[Stream[Failure, T]](stream)(stream.timeoutFail(Idle))

    // Probably there is a better way to code this.
    val EntityDescription(_, entityID, _, _, encoder, _, _, _) = shardGroup.entityDescription

    // TODO: Inline
    private def tellBytes(id: ID, partition: Partition, event: ByteArray): UIO[UIO[Done]] = {

      val record =
        new ProducerRecord(
          topic,
          partition,
          id,
          event)

      producer
        .produceAsync(record, Serializer.string, Serializer.byteArray)
        .map(_.orDie.unit)
        .orDie
    }

    private def tellBytes(id: ID, event: ByteArray): UIO[UIO[Partition]] =
      producer
        .produceAsync(
          topic,
          id,
          event,
          Serializer.string,
          Serializer.byteArray)
        .map(_.orDie.map(_.partition()))
        .orDie

    private[sharding] def tell(id: ID, partition: Partition, event: T): UIO[UIO[Done]] =
      serializeShardEvent(encoder)(entityID, id, event, reply = true)
        .flatMap(tellBytes(id, partition, _))
        .orDie

    def tell(id: ID, event: T): UIO[UIO[Partition]] =
      serializeShardEvent(encoder)(entityID, id, event, reply = false)
        .flatMap(tellBytes(id, _))
        .orDie

  }

}

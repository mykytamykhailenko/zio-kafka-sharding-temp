package zio.kafka.sharding.model

import zio._
import zio.concurrent.ConcurrentMap
import zio.kafka.sharding.{Done, Entity, Failure, Timeout}
import zio.kafka.sharding.Entity.ID


/**
 * ''Shard'' contains all entities belonging to the same partition.
 *
 * Once the partition is descheduled, all entities are terminated.
 *
 * ''Shard'' keeps track of idle entities and terminates them
 * when they are inactive for long enough time.
 *
 * ''Shard'' contains ''reply'' entities.
 * ''Reply'' entities are created separately.
 * The proper way to shut them down is to complete the ''rebalance''
 * promise.
 *
 * @param entities Entities
 * @param expired  Expired entities
 * @tparam T The entity type
 */
private[sharding]
case class Shard[T](entities: ConcurrentMap[ID, Entity[T]],
                    expired: Queue[ID],
                    rebalance: Promise[Nothing, Done]) {

  def shutdown(backpressure: Boolean, terminateTimeout: Duration): IO[Failure, Done] = for {

    _ <- rebalance.succeed()

    chunk <- entities.toChunk

    _ <- entities.removeIf((_, _) => true)

    _ <- chunk.collectZIO { case (_, entity) =>
      entity.shutdown(backpressure).timeoutFail(Timeout)(terminateTimeout)
    }

    _ <- expired.shutdown

  } yield ()

  def shutdownIdleEntities(backpressure: Boolean, terminateTimeout: Duration): IO[Failure, Done] = for {

    idle <- expired.takeAll

    _ <-
      idle
        .collectZIO(id =>
          entities
            .remove(id)
            .flatMap(
              _.fold[IO[Failure, Done]](ZIO.unit)(
                _.shutdown(backpressure).timeoutFail(Timeout)(terminateTimeout))
            )
        )

  } yield ()

}

object Shard {

  def make[T]: UIO[Shard[T]] =
    for {
      entities <- ConcurrentMap.empty[ID, Entity[T]]
      expired <- Queue.unbounded[ID]
      rebalance <- Promise.make[Nothing, Done]
    } yield Shard(entities, expired, rebalance)

}

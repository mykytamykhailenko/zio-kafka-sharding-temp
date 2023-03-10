package zio.kafka.sharding.model

import zio._
import zio.concurrent.ConcurrentMap
import zio.kafka.sharding.{Done, Failure, Partition, SameShardRegisteredTwice}

private[sharding]
case class ShardGroup[T](shards: ConcurrentMap[Partition, Shard[T]],
                         entityDescription: EntityDescription[T],
                         private val queue: Queue[Partition],
                         private val counter: Ref[Int]) {

  def put(partition: Partition, shard: Shard[T]) =
    shards.putIfAbsent(partition, shard)
      .flatMap(_.fold[IO[Failure, Done]](ZIO.unit)(_ => ZIO.fail(SameShardRegisteredTwice))) *> queue.offer(partition)

  def remove(partition: Partition) = shards.remove(partition)

  def getShardRoundRobin: UIO[(Partition, Shard[T])] =
    for {
      round <- counter.getAndUpdate(_ + 1)
      pieces <- shards.toChunk
      shard <- if (pieces.nonEmpty) ZIO.succeed(pieces(round % pieces.size)) else queue.take *> getShardRoundRobin
    } yield shard

}

object ShardGroup {

  def make[T](entityDescription: EntityDescription[T]): UIO[ShardGroup[T]] =
    for {
      shards <- ConcurrentMap.empty[Partition, Shard[T]]
      queue <- Queue.unbounded[Partition]
      counter <- Ref.make(0)
    } yield ShardGroup(shards, entityDescription, queue, counter)


}
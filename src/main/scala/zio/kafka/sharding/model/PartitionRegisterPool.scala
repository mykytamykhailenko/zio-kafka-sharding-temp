package zio.kafka.sharding.model

import zio.{UIO, ZIO}
import zio.kafka.sharding.Entity.ID
import zio.kafka.sharding.PartitionRegisterer.ShardEventStream
import zio.kafka.sharding.Sharding.ShardingDescription
import zio.kafka.sharding.{Done, Failure, ID, Partition, PartitionRegisterer, Sharding}
import zio.stream.{Stream, ZStream}

case class PartitionRegisterPool(pool: Map[ID, PartitionRegisterer[_]]) {

  def apply(partition: Partition)(entityID: ID, partitionStream: ShardEventStream): Stream[Failure, Done] =
    ZStream.fromZIO(
      pool(entityID)
        .registerPartitionAwait(partition, partitionStream))

  def registerShardGroups: UIO[Done] = ZIO.collectAllParDiscard(pool.values.map(_.createShardGroup))

}

object PartitionRegisterPool {

  def apply(sharding: Sharding, shardingDescription: ShardingDescription, entityDescriptions: Seq[EntityDescription[_]]): PartitionRegisterPool =
    PartitionRegisterPool(entityDescriptions.map(description =>
      description.entityID -> PartitionRegisterer(description, shardingDescription, sharding))
      .toMap)

}
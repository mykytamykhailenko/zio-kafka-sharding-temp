
import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import zio._
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.sharding.Entity.{Behavior, ID}
import zio.kafka.sharding.Sharding
import zio.kafka.sharding.Sharding.ShardingDescription
import zio.kafka.sharding.model.EntityDescription

object CounterApp extends ZIOAppDefault {

  private val consumerSettings = ConsumerSettings(List("localhost:9092"))
    .withGroupId("sharding")

  private val subscription = Subscription.topics("sharded")

  // Macros does not work well with for-comprehensions.
  private val consumer =
    Consumer.make(consumerSettings)
      .flatMap(consumer => consumer.subscribe(subscription)
        .map(_ => consumer))

  val consumerScopedLayer: TaskLayer[Consumer] = ZLayer.scoped(consumer)

  private val producerSettings = ProducerSettings(List("localhost:9092"))

  val producerScopedLayer: TaskLayer[Producer] = ZLayer.scoped(Producer.make(producerSettings))

  sealed trait ReplicateCommand
  case object Replicate extends ReplicateCommand

  val behavior: Behavior[ReplicateCommand] =
    (id: ID) => {

      for {
        shard <- ZIO.serviceWithZIO[Sharding](_.getShard[ReplicateCommand].orDieWith(_ => new Throwable("Cannot get shard group")))
        _ <- ZIO.logInfo(s"[Entity-$id] Created!")
      } yield (_: ReplicateCommand) => ZIO.sleep(1.second) *> shard.tell((id.toInt + 1).toString, Replicate).map(_ => ZIO.succeed())
    }

  val schemaFor = SchemaFor[ReplicateCommand]
  val encoder = Encoder[ReplicateCommand].withSchema(schemaFor)
  val decoder = Decoder[ReplicateCommand].withSchema(schemaFor)

  override def run = (
    for {
      producer <- ZIO.service[Producer]
      consumer <- ZIO.service[Consumer]

      sharding <- Sharding.make(
        ShardingDescription(),
        consumer,
        producer,
        EntityDescription("replicator", behavior, None, encoder, decoder))

      shard <- sharding.getShard[ReplicateCommand]

      _ <- ZIO.sleep(3.seconds)

      _ <- shard.tell("0", Replicate)

      _ <- ZIO.never
    } yield ()
  ).provide(consumerScopedLayer, producerScopedLayer)



}





















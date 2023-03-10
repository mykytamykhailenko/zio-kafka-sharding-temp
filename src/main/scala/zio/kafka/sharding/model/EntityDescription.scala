package zio.kafka.sharding.model

import com.sksamuel.avro4s.{Decoder, Encoder}
import zio._
import zio.kafka.sharding.Entity._
import zio.kafka.sharding.{Entity, Sharding}

import scala.reflect.runtime.universe._

/**
 * An entity description.
 *
 * The behavior is the description how to create an entity.
 * The entity has a unique ID.
 * The entity can access other entities via ''Sharding''.
 * The entity can define its state in the outer effect.
 * The entity will be automatically restarted once it is interrupted via ''ZIO.interrupt''.
 * Once the partition get descheduled from this pod, all entities belonging to it
 * will receive ''terminate'' event, and all entities will be restarted on another pod.
 * It is highly recommended to define ''terminate'' for such cases.
 *
 * Example:
 * {{{
 * sealed trait CounterEvent
 * case object Increment extends CounterEvent
 * case object Decrement extends CounterEvent
 * case object Restart extends CounterEvent
 *
 * // The inner layer is the 'waiting' layer.
 * // In this case, there is no need to wait for anything,
 * // but we must conform the type signature.
 * def raise[R, E, A](zio: ZIO[R, E, A]) = zio.map(_ => ZIO.succeed())
 *
 * val behavior: ID => URIO[Sharding, Behavior[CounterEvent]] =
 *   (_: ID) =>
 *     ZIO.serviceWithZIO(_ =>
 *       for {
 *         counter <- Ref.make(0)
 *       } yield (event: CounterEvent) =>
 *         event.event match {
 *           case Increment => raise(counter.update(_ + 1))
 *           case Decrement => raise(counter.update(_ - 1))
 *           case Restart => ZIO.interrupt
 *         }
 *     )
 * }}}
 *
 * The events get serialized by ''avro4s''.
 * Here is an example how to properly define ''Encoder'' and ''Decoder''.
 *
 * {{{
 *   val schemaFor = SchemaFor[CounterEvent]
 *   val encoder = Encoder[CounterEvent].withSchema(schemaFor)
 *   val decoder = Decoder[CounterEvent].withSchema(schemaFor)
 * }}}
 *
 * See [[Sharding]] and [[Entity.Behavior Behavior]] for more info.
 *
 * NOTE:
 * Depending on the backpressure, the stream can be significantly slowed down.
 * Please, see [[Entity.Behavior Behavior]] for more details.
 *
 * @param behavior         The behavior
 * @param terminate        The terminate message
 * @param encoder          Encoder
 * @param decoder          Decoder
 * @param idleTimeout      Idle timeout
 * @param terminateTimeout Terminate timeout
 * @tparam T The entity protocol
 */
case class EntityDescription[T] private[sharding](private[sharding] val typeTag: TypeTag[T],
                                                  entityID: ID,
                                                  behavior: Behavior[T],
                                                  terminate: Option[T],
                                                  encoder: Encoder[T],
                                                  decoder: Decoder[T],
                                                  idleTimeout: Duration,
                                                  terminateTimeout: Duration)

object EntityDescription {

  def apply[T](entityID: ID,
               behavior: Behavior[T],
               terminate: Option[T],
               encoder: Encoder[T],
               decoder: Decoder[T],
               idleTimeout: Duration = 15.minutes,
               terminateTimeout: Duration = 30.seconds)(implicit typeTag: TypeTag[T]): EntityDescription[T] =

    EntityDescription(
      typeTag,
      entityID,
      behavior,
      terminate,
      encoder,
      decoder,
      idleTimeout,
      terminateTimeout)

}
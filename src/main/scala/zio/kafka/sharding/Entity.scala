package zio.kafka.sharding

import zio._
import zio.kafka.sharding.Entity._


/**
 * An entity.
 *
 * @param id        Entity's ID
 * @param queue     Entity's queue
 * @param behavior  Behavior
 * @param receive   Receiver
 * @param terminate An event for terminating an entity
 * @param semaphore Queue's semaphore
 * @tparam T Entity type
 */
// I need to add .uninterruptible or .uninterruptibleMask
private[sharding] case class Entity[T](id: ID,
                                       private val queue: Queue[T],
                                       private val behavior: Behavior[T],
                                       private val receive: Ref[Option[Receive[T]]],
                                       private val trigger: UIO[Done],
                                       private val terminate: Option[T],
                                       private val semaphore: Semaphore) {


  private def execute[R, E, A](info: String)(zio: ZIO[R, E, A]): ZIO[R, E, Done] = {

    val executed = semaphore.withPermit {
      for {
        shutdown <- queue.isShutdown
        _ <- ZIO.unless(shutdown)(zio)
        _ <- ZIO.when(shutdown)(ZIO.logInfo(info))
      } yield ()
    }

    executed
      .tapDefect(cause => ZIO.logCause(s"[Entity-$id] Failed because of a defect", cause))
      .catchAllCause(_ => ZIO.unit)
  }

  def offerAll(events: Iterable[T]): UIO[Done] =
    execute(
      s"[Entity-$id] Received some events after being terminated: ${events.mkString(", ")}"
    )(queue.offerAll(events))

  private def deliver(receive: T => UIO[UIO[Done]],
                      event: T,
                      backpressure: Boolean): UIO[Done] =
    if (backpressure) trigger *> receive(event).flatten
    else trigger *> receive(event).unit


  /**
   * Shutdown the queue and terminate the entity.
   *
   * The entity may be in progress while this method gets called.
   * In such case, the entity finishes processing current event,
   * and only then, it stops.
   *
   * You should use this method to terminate all entities
   * belonging to a reassigned partition.
   *
   * This method terminates the underlying actor,
   * but does not remove the entity from
   * the entity list.
   *
   * The end user should take care of this.
   *
   * @return Effect
   */
  def shutdown(backpressure: Boolean): UIO[Done] =
    execute(s"[Entity-$id] 'shutdown' called after being terminated") {

      for {
        _ <- queue.shutdown

        receiver <- receive.get

        delivery = receiver.fold(ZIO.unit)(receive =>
          terminate.fold(ZIO.unit)(end =>
            deliver(receive, end, backpressure)
          ))

        _ <- delivery.catchAllCause(cause =>
          if (cause.isInterruptedOnly) receive.set(None)
          else ZIO.failCause(cause))

      } yield ()
    }

  /**
   * Receive N events.
   *
   * The underlying will process N events,
   * then, it will stop consuming any new messages.
   *
   * You may call ''shutdown'' while being in progress.
   * This will stop the actor from processing any new message,
   * but it won't interrupt the current message.
   *
   * @param n The number of events,
   * @return Effect
   */
  def receiveN(n: Int, backpressure: Boolean): URIO[Sharding, Done] = {

    val restartAndGet = for {

      receiver <- behavior(id)

      _ <- receive.set(Some(receiver))

    } yield receiver

    val receiveOrRestart = for {

      event <- queue.take

      receiver <- receive.get

      receiving <- receiver.fold(restartAndGet)(receiving => ZIO.succeed(receiving))

      _ <-
        deliver(receiving, event, backpressure)
          .catchAllCause(cause =>
            if (cause.isInterruptedOnly) receive.set(None)
            else ZIO.failCause(cause))

    } yield ()

    ZIO.when(n > 0)(
      execute(s"[Entity-$id] 'receiveN' has been called after being terminated")(receiveOrRestart) *>
        receiveN(n - 1, backpressure)).unit
  }

}

object Entity {

  type ID = String

  /**
   * An entity behavior.
   *
   * The behavior describes how an entity should react to an event.
   *
   * The entity event stream is backpressured by default.
   * The stream will consume a new chunk of events only after
   * the slowest entity is done with its events.
   * Then, it will submit latest offset.
   * This is an important consideration, because
   * it prevents the application from consuming too many events.
   * It is possible to disable backpressure,
   * in this case, the stream will ensure the entity receives all of its events,
   * but it won't await for the effect to complete.
   * Also, it will not submit the offset in such case.
   * This behavior is reflected in the nested type signature.
   *
   * Example:
   * {{{
   *  def blocking(event: String): UIO[Promise[Nothing, Unit]] = ???
   *
   *  val behavior: String => UIO[UIO[Unit]] =
   *    event =>
   *      blocking(event)
   *        .map(promise => promise.await)
   * }}}
   *
   * When backpressured, the stream will await for ''blockingEffect'',
   * while when backpressured is disabled, it will not.
   *
   * The end user must ensure the entity does not fail because of defects.
   * Thought, it is acceptable to interrupt the behavior with
   * ''ZIO.interrupt''
   *
   */
  type Receive[T] = T => UIO[UIO[Done]]

  type Behavior[T] = ID => URIO[Sharding, Receive[T]]


  /**
   * Creates an entity.
   *
   * ''trigger'' is executed each time an entity receives an event.
   * Use ''trigger'' to drop idle events.
   *
   * See [[Sharding]], [[Entity.Behavior Behavior]],
   * and [[zio.kafka.sharding.model.EntityDescription EntityDescription]] for more details.
   *
   * @param id        An entity ID
   * @param behavior  The entity behavior
   * @param trigger   The trigger
   * @param terminate The termination message
   * @tparam T The entity protocol
   * @return Effect
   */
  private[sharding] def createEntity[T](id: ID,
                                        behavior: Behavior[T],
                                        trigger: UIO[Done] = ZIO.unit,
                                        terminate: Option[T] = None): URIO[Sharding, Entity[T]] =
    for {
      semaphore <- Semaphore.make(1)
      queue <- Queue.unbounded[T]
      receiver <- Ref.make[Option[Receive[T]]](None)
    } yield Entity(id, queue, behavior, receiver, trigger, terminate, semaphore)

}

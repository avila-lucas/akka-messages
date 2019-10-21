package com.omnipresent.model

import akka.actor.{Actor, ActorLogging, Props, _}
import akka.routing._
import com.omnipresent.model.Consumer.{ConsumedJob, Job}
import com.omnipresent.model.MessagesQueueProxy.{FailedReception, TimedOut}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.{FiniteDuration, _}

object MessagesQueueProxy {

  final case class Produce(interval: FiniteDuration)

  final case class Start(interval: FiniteDuration)

  final case object TimedOut

  final case class FailedReception(job: Job)

  final case class Rejected(id: String)

  def props: Props = Props[MessagesQueueProxy]
}

/**
 * This is kind of a proxy, it handles all things related to sending messages to the workers. For that puporse it creates
 * a WaitingConfirmator (I don't think that word exists) and delagates the task of waiting for the workers' confirmation
 * (depending on the spread type).
 */
class MessagesQueueProxy(spreadType: String, workers: Int)
  extends Actor
    with ActorLogging {

  val spread: RoutingLogic = if (spreadType.equalsIgnoreCase("PubSub")) BroadcastRoutingLogic() else RoundRobinRoutingLogic()
  var consumersRouter: Router = createRouter(Props[Consumer], "consumer", spread, workers)

  def receive: Receive = {
    case job: Job =>
      val queue = sender()
      context.actorOf(Props(new WaitingConfirmator(consumersRouter, job, queue)))
    case _ => // TODO
  }

  private def createRouter(props: Props, nameType: String, routingLogic: RoutingLogic, quantity: Int) = {
    val routees = (1 to quantity).map { idx =>
      val r = context.actorOf(props, s"${nameType}_$idx")
      context.watch(r)
      ActorRefRoutee(r)
    }
    Router(routingLogic, routees)
  }
}

class WaitingConfirmator(router: Router, job: Job, queue: ActorRef)
  extends Actor
    with ActorLogging {

  implicit val ex: ExecutionContextExecutor = context.system.dispatcher
  var confirmationList = List.empty[ConsumedJob]
  val timeoutScheduled: Cancellable = context.system.scheduler.scheduleOnce(20.seconds, self, TimedOut)

  override def preStart(): Unit = router.route(job, self)

  override def postStop(): Unit = timeoutScheduled.cancel()

  override def receive: Receive = {

    case consumed: ConsumedJob =>
      log.info(s"[${job.jobId}] ACK")
      confirmationList = consumed :: confirmationList
      answerIfDone(consumed)

    case TimedOut =>
      log.warning(s"[${job.jobId}] TIMED OUT")
      queue ! FailedReception(job)
      context stop self

  }

  private def answerIfDone(consumedJob: ConsumedJob) {
    router.logic match {
      case _: BroadcastRoutingLogic if confirmationList.size == router.routees.size =>
        acknowledge(consumedJob)
      case _ => acknowledge(consumedJob)
    }
  }

  private def acknowledge(consumedJob: ConsumedJob) {
    log.info(s"[${job.jobId}] ACKNOWLEDGE")
    queue ! consumedJob
    context stop self
  }

}

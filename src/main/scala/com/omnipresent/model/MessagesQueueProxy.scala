package com.omnipresent.model

import akka.actor.{ Actor, ActorLogging, Props, _ }
import akka.routing._
import com.omnipresent.model.Consumer.{ ConfirmReception, PerformCalculation }
import com.omnipresent.model.MessagesQueueProxy.{ FailedReception, TimedOut }
import org.apache.commons.lang3.time.StopWatch

import scala.concurrent.duration.{ FiniteDuration, _ }

object MessagesQueueProxy {

  final case class Produce(interval: FiniteDuration)

  final case class Start(interval: FiniteDuration)

  final case object TimedOut

  final case class FailedReception(job: PerformCalculation)

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
    case j: PerformCalculation =>
      val jobForConsumer = PerformCalculation(j.jobId, j.deliveryId, StopWatch.createStarted())
      val queue = sender()
      context.actorOf(Props(new WaitingConfirmator(consumersRouter, jobForConsumer, queue)))
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

class WaitingConfirmator(router: Router, job: PerformCalculation, queue: ActorRef) extends Actor with ActorLogging {
  var valueResults = List.empty[ConfirmReception]
  implicit val ex = context.system.dispatcher

  val timeoutScheduled = context.system.scheduler.scheduleOnce(5.seconds, self, TimedOut)

  override def preStart(): Unit = {
    router.route(job, self)
    log.info(s"[${job.jobId}] SENT to workers")
  }

  override def receive: Receive = {
    case confirmed: ConfirmReception =>
      log.info(s"[${job.jobId}] ACK")
      valueResults = confirmed :: valueResults
      answerIfDone(confirmed)
    case TimedOut =>
      log.warning(s"[${job.jobId}] TIMEDOUT")
      queue ! FailedReception(job)
      context stop self
  }

  def answerIfDone(confirmation: ConfirmReception) = {
    if (valueResults.size == router.routees.size) {
      log.info(s"[${job.jobId}] ALL ACKs")
      timeoutScheduled.cancel()
      queue ! confirmation
      context stop self
    }
  }
}

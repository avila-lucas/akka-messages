package com.omnipresent.model

import java.util.UUID

import akka.actor.{ Actor, ActorLogging, Props, _ }
import akka.cluster.sharding.ClusterSharding
import akka.routing._
import com.omnipresent.model.Consumer.{ ConsumedJob, Job }
import com.omnipresent.model.MessagesQueue.ProxyJob
import com.omnipresent.model.MessagesQueueProxy.{ FailedReception, TimedOut }

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

object MessagesQueueProxy {

  final case object TimedOut

  final case class FailedReception(jobId: String)

  final case class Rejected(id: String)

  def props: Props = Props[MessagesQueueProxy]
}

class MessagesQueueProxy(queueName: String, spreadType: String, workers: Int)
  extends Actor
  with ActorLogging {

  var workersNames: List[String] = List.empty
  var lastVisitedWorker: Int = 0
  val spread: RoutingLogic = if (spreadType.equalsIgnoreCase("PubSub")) BroadcastRoutingLogic() else RoundRobinRoutingLogic()
  private val consumerRegion: ActorRef = ClusterSharding(context.system).shardRegion(Consumer.shardName)

  override def preStart(): Unit = {
    workersNames = 1 to workers map {
      idx =>
        s"worker-${idx}_${UUID.randomUUID().getMostSignificantBits}"
    } toList

    log.info(s"Messages queue proxy started with [${workersNames.size}] workers")
  }

  def receive: Receive = {
    case job: ProxyJob =>
      val queue = sender()
      spread match {
        case _: BroadcastRoutingLogic => // TODO: Improvement - Composition :)
          sendJob(job, workersNames, queue)
        case _: RoundRobinRoutingLogic =>
          sendJob(job, List(workersNames(lastVisitedWorker)), queue)
      }
      lastVisitedWorker += 1
      if (lastVisitedWorker >= workersNames.size)
        lastVisitedWorker = 0
    case _ => // TODO
  }

  private def sendJob(job: ProxyJob, workers: List[String], replyTo: ActorRef) = {
    val waitingConfirmator = context.actorOf(Props(new WaitingConfirmator(workers, job.jobId, replyTo)))
    workers map {
      name =>
        consumerRegion ! Job(
          consumerName = name,
          jobId = job.jobId,
          deliveryId = job.deliveryId,
          watch = job.watch,
          transactional = job.transactional,
          replyTo = waitingConfirmator)
    }
  }

}

class WaitingConfirmator(workers: List[String], jobId: String, queue: ActorRef)
  extends Actor
  with ActorLogging {

  implicit val ex: ExecutionContextExecutor = context.system.dispatcher
  var confirmedJobs: Int = 0
  val timeoutScheduled: Cancellable = context.system.scheduler.scheduleOnce(20.seconds, self, TimedOut)

  override def postStop(): Unit = timeoutScheduled.cancel()

  override def receive: Receive = {

    case consumed: ConsumedJob =>
      log.info(s"[${consumed.jobId}] ACK")
      confirmedJobs += 1
      answerIfDone(consumed)

    case TimedOut =>
      log.warning(s"[$jobId] TIMED OUT")
      queue ! FailedReception(jobId)
      context stop self

  }

  private def answerIfDone(consumedJob: ConsumedJob): Unit = if (confirmedJobs == workers.size) acknowledge(consumedJob)

  private def acknowledge(consumedJob: ConsumedJob) {
    log.info(s"[${consumedJob.jobId}] ACKNOWLEDGE")
    queue ! consumedJob
    context stop self
  }

}

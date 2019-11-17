package com.omnipresent.model

import akka.actor.{Actor, ActorLogging, Props, _}
import akka.cluster.sharding.ClusterSharding
import akka.routing._
import com.omnipresent.model.Consumer.{ConsumedJob, Job}
import com.omnipresent.model.MessagesQueue.{ProxyJob, RetryJob}
import com.omnipresent.model.MessagesQueueProxy.{FailedReception, TimedOut}
import com.omnipresent.support.IdGenerator

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

object MessagesQueueProxy {

  final case object TimedOut

  final case class FailedReception(job: ProxyJob, retryWorkers: List[String])

  final case class Rejected(id: String)

  def props: Props = Props[MessagesQueueProxy]
}

class MessagesQueueProxy(queueName: String,
                         spreadType: String,
                         workers: Int)
  extends Actor
    with ActorLogging {

  var workersNames: List[String] = List.empty
  var nextWorkerToVisit: Int = 0
  val spread: RoutingLogic = if (spreadType.equalsIgnoreCase("PubSub")) BroadcastRoutingLogic() else RoundRobinRoutingLogic()
  private val consumerRegion: ActorRef = ClusterSharding(context.system).shardRegion(Consumer.shardName)

  override def preStart(): Unit = {
    workersNames = 1 to workers map {
      idx =>
        s"${queueName}-WORKER-${idx}"
    } toList

    log.info(s"Messages queue proxy started with [${workersNames.size}] workers")
  }

  def receive: Receive = {

    /** Main dispatcher job accordingly to spread type **/
    case job: ProxyJob =>
      val queue = sender()

      spread match { // TODO: Improvement with Composition :)
        case _: BroadcastRoutingLogic =>
          sendJob(job, workersNames, queue)
        case _: RoundRobinRoutingLogic =>
          sendJob(job, List(workersNames(nextWorkerToVisit)), queue)
          updateNextWorkerToVisit()
      }

    /** To retry job with some specific workers **/
    case RetryJob(job, retryWorkers) =>
      sendJob(job, retryWorkers, sender())

    case _ => // TODO
  }

  private def sendJob(job: ProxyJob, workers: List[String], replyTo: ActorRef) = {
    val waitingConfirmator = context.actorOf(Props(new WaitingConfirmator(workers, job, replyTo)))
    workers map {
      name =>
        consumerRegion ! Job(
          consumerName = IdGenerator.getRandomID(name),
          jobId = job.jobId,
          replyTo = waitingConfirmator)
    }
  }

  private def updateNextWorkerToVisit() {
    nextWorkerToVisit += 1
    if (nextWorkerToVisit >= workersNames.size)
      nextWorkerToVisit = 0
  }

}

class WaitingConfirmator(workers: List[String],
                         job: ProxyJob,
                         queue: ActorRef)
  extends Actor
    with ActorLogging {

  implicit val ex: ExecutionContextExecutor = context.system.dispatcher
  val timeoutScheduled: Cancellable = context.system.scheduler.scheduleOnce(20.seconds, self, TimedOut)
  var confirmedJobs: List[String] = List.empty

  override def postStop(): Unit = timeoutScheduled.cancel()

  override def receive: Receive = {

    case consumed: ConsumedJob =>
      confirmedJobs = sender().path.name :: confirmedJobs
      log.info(s"[${consumed.jobId}] ACK, killing ${sender().path.name}")
      sender() ! PoisonPill
      answerIfDone(consumed)

    case TimedOut =>
      val failedReceptionFrom = workers.diff(confirmedJobs)
      log.warning(s"[${job.jobId}] TIMED OUT, failed to receive response from: ${failedReceptionFrom.mkString("[", ",", "]")}")
      queue ! FailedReception(job, failedReceptionFrom)
      context stop self

  }

  private def answerIfDone(consumedJob: ConsumedJob): Unit = if (confirmedJobs.size == workers.size) acknowledge(consumedJob)

  private def acknowledge(consumedJob: ConsumedJob) {
    log.info(s"[${consumedJob.jobId}] ACKNOWLEDGE")
    queue ! consumedJob
    context stop self
  }

}

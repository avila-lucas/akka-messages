package com.omnipresent.model

import akka.actor.{ Actor, ActorLogging, _ }
import com.omnipresent.model.Consumer.ConsumedJob
import com.omnipresent.model.MessagesQueue.{ ProxyJob, QueueConsumedJob }
import com.omnipresent.model.MessagesQueueProxy.{ FailedReception, TimedOut }

import scala.concurrent.duration._
import scala.concurrent.ExecutionContextExecutor

class WaitingConfirmator(
  workers: List[String],
  job: ProxyJob,
  queueName: String,
  queueRegion: ActorRef)
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
      queueRegion ! FailedReception(queueName, job, failedReceptionFrom)
      context stop self

  }

  private def answerIfDone(consumedJob: ConsumedJob): Unit = if (confirmedJobs.size == workers.size) acknowledge(consumedJob)

  private def acknowledge(consumedJob: ConsumedJob) {
    log.info(s"[${consumedJob.jobId}] ACKNOWLEDGE")
    queueRegion ! QueueConsumedJob(queueName, consumedJob.jobId)
    context stop self
  }
}

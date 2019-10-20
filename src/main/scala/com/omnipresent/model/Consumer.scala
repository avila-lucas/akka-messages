package com.omnipresent.model

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, Props}
import org.apache.commons.lang3.time.StopWatch

object Consumer {

  final case class Job(jobId: String, deliveryId: Long, watch: StopWatch, transactional: Boolean)

  case class ConsumedJob(jobId: String, deliveryId: Long)

  def props: Props = Props[Consumer]
}

class Consumer extends Actor with ActorLogging {

  import Consumer._

  var latestJob: Option[Job] = None

  def receive: Receive = {
    case job: Job if latestJob.exists(_.equals(job)) =>
      log.info(s"[${job.jobId}] ALREADY CONSUMED")
      sender() ! ConsumedJob(job.jobId, job.deliveryId)

    case job@Job(id, deliveryId, watch, transactional) =>
      log.info(s"[$id] RECEIVED (consumer)")
      if (!transactional) sender() ! ConsumedJob(id, deliveryId)

      Thread.sleep(1000)

      log.info(s"[$id] DONE in [${watch.getTime(TimeUnit.SECONDS)}] seconds!")
      if (transactional) sender() ! ConsumedJob(id, deliveryId)
      latestJob = Some(job)

    case _ => // TODO
  }

}


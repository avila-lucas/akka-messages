package com.omnipresent.model

import java.util.concurrent.TimeUnit

import akka.actor.{ Actor, ActorLogging, Props }
import akka.cluster.sharding.ShardRegion
import org.apache.commons.lang3.time.StopWatch

object Consumer {

  final case class Job(consumerName: String, jobId: String, deliveryId: Long, watch: StopWatch, transactional: Boolean)

  final case class ConsumedJob(jobId: String, deliveryId: Long)

  def props(): Props = Props[Consumer]

  val entityIdExtractor: ShardRegion.ExtractEntityId = {
    case j: Job => (j.consumerName, j)
  }

  val shardIdExtractor: ShardRegion.ExtractShardId = {
    case j: Job => (math.abs(j.consumerName.split("_").last.toInt.hashCode) % 100).toString
  }

  val shardName: String = "Consumers"
}

class Consumer
  extends Actor
  with ActorLogging {

  import Consumer._

  var latestJob: Option[Job] = None

  def receive: Receive = {
    case job: Job if latestJob.exists(_.equals(job)) =>
      log.info(s"[${job.jobId}] ALREADY CONSUMED")
      sender() ! ConsumedJob(job.jobId, job.deliveryId)

    case job @ Job(_, id, deliveryId, watch, transactional) =>
      log.info(s"[$id] RECEIVED (consumer)")
      if (!transactional) sender() ! ConsumedJob(id, deliveryId)

      Thread.sleep(1000)

      log.info(s"[$id] DONE in [${watch.getTime(TimeUnit.SECONDS)}] seconds!")
      if (transactional) sender() ! ConsumedJob(id, deliveryId)
      latestJob = Some(job)

    case _ => // TODO
  }

}


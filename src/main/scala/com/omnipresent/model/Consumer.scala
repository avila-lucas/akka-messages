package com.omnipresent.model

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.cluster.sharding.ShardRegion

object Consumer {

  final case class Job(consumerName: String, jobId: String, replyTo: ActorRef)

  final case class ConsumedJob(jobId: String)

  def props(): Props = Props[Consumer]

  val entityIdExtractor: ShardRegion.ExtractEntityId = {
    case j: Job => (j.consumerName, j)
  }

  val shardIdExtractor: ShardRegion.ExtractShardId = {
    case j: Job => (math.abs(j.consumerName.split("_").last.toLong.hashCode) % 100).toString
  }

  val shardName: String = "Consumers"
}

class Consumer
  extends Actor
  with ActorLogging {

  import Consumer._

  val transactional = true // FIXME - Make sharding region for transactional and not transactional consumers
  var latestJob: Option[Job] = None

  def receive: Receive = {
    case job: Job if !transactional && latestJob.exists(_.equals(job)) =>
      log.info(s"[${job.jobId}] ALREADY CONSUMED")
      sender() ! ConsumedJob(job.jobId)

    case job @ Job(_, id, replyTo) =>
      log.info(s"[$id] RECEIVED (consumer)")
      if (!transactional) replyTo ! ConsumedJob(id)

      Thread.sleep(1000)

      log.info(s"[$id] DONE")
      latestJob = Some(job)
      if (transactional) replyTo ! ConsumedJob(id)

    case _ => // TODO
  }

}


package com.omnipresent.model

import akka.actor.{ Actor, ActorLogging, Props }
import akka.cluster.sharding.{ ClusterSharding, ShardRegion }
import akka.remote.ContainerFormats.ActorRef

object Consumer {

  final case class Job(consumerName: String, jobId: String, waitingShardName: String)

  final case class ConsumedJob(jobId: String)

  def props(transactional: Boolean): Props = Props(new Consumer(transactional))

  val entityIdExtractor: ShardRegion.ExtractEntityId = {
    case j: Job => (j.consumerName, j)
  }

  val shardIdExtractor: ShardRegion.ExtractShardId = {
    case j: Job => (math.abs(j.consumerName.split("-").last.toLong.hashCode) % 100).toString
    case ShardRegion.StartEntity(id) ⇒ (math.abs(id.split("-").last.toLong.hashCode) % 100).toString
  }

  val transactionalShardName: String = "Consumers-Transactional"
  val nonTransactionalShardName: String = "Consumers-Non-Transactional"

}

class Consumer(transactional: Boolean)
  extends Actor
  with ActorLogging {

  import Consumer._

  def receive: Receive = {
    case Job(_, id, waitingShardName) =>
      val replyTo = ClusterSharding(context.system).shardRegion(waitingShardName)
      log.info(s"[$id] RECEIVED (consumer)")
      if (!transactional) replyTo ! ConsumedJob(id)

      Thread.sleep(1000)

      log.info(s"[$id] DONE")
      if (transactional) replyTo ! ConsumedJob(id)

    case _ => // TODO
  }

}


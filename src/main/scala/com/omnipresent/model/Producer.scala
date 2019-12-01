package com.omnipresent.model

import java.time.{ LocalDateTime, ZoneOffset }
import java.util.UUID

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.cluster.sharding.{ ClusterSharding, ShardRegion }
import com.omnipresent.model.MessagesQueueProxy.Rejected
import com.omnipresent.model.Producer.{ DeliverJob, Produce }

import scala.concurrent.duration.FiniteDuration

object Producer {

  final case class Produce(producerName: String, queueName: String, spreadType: String, interval: FiniteDuration)

  final case class DeliverJob(id: String, queueName: String)

  def props(): Props = Props[Producer]

  val entityIdExtractor: ShardRegion.ExtractEntityId = {
    case p: Produce => (p.producerName, p)
  }

  val shardIdExtractor: ShardRegion.ExtractShardId = {
    case p: Produce => (math.abs(p.producerName.split("-").last.toLong.hashCode) % 100).toString
    case ShardRegion.StartEntity(id) ⇒ (math.abs(id.split("-").last.toLong.hashCode) % 100).toString
  }

  val shardName: String = "Producers"

}

class Producer
  extends Actor
  with ActorLogging {

  override def receive: Receive = {
    case Produce(_, queueName, spreadType, interval) =>
      val queueShardName: String = if (spreadType.equalsIgnoreCase("PubSub")) MessagesQueue.pubSubShardName else MessagesQueue.broadcastShardName
      val queueRegion: ActorRef = ClusterSharding(context.system).shardRegion(queueShardName)
      produce(queueName, interval, queueRegion)
    case r: Rejected =>
      log.info(s"Job [${r.id}] REJECTED :(")
    case _ => // TODO
  }

  def produce(queueName: String, interval: FiniteDuration, queueRegion: ActorRef) {
    val id = s"${UUID.randomUUID().toString}-${LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli()}"
    log.info(s"[$id] Job PRODUCED")
    queueRegion ! DeliverJob(id, queueName)

    Thread.sleep(interval.toMillis)

    produce(queueName, interval, queueRegion)
  }

}
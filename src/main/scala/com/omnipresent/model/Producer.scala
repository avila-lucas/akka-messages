package com.omnipresent.model

import java.util.UUID

import akka.actor.{ Actor, ActorLogging, Props }
import akka.cluster.sharding.ShardRegion
import com.omnipresent.model.MessagesQueueProxy.Rejected
import com.omnipresent.model.Producer.{ DeliverJob, Produce }

import scala.concurrent.duration.FiniteDuration

object Producer {

  final case class Produce(producerName: String, queueName: String, interval: FiniteDuration)

  final case class DeliverJob(id: String, queueName: String)

  def props(): Props = Props[Producer]

  val entityIdExtractor: ShardRegion.ExtractEntityId = {
    case p: Produce => (p.producerName, p)
  }

  val shardIdExtractor: ShardRegion.ExtractShardId = {
    case p: Produce => (math.abs(p.producerName.split("_").last.toLong.hashCode) % 100).toString
  }

  val shardName: String = "Producers"

}

class Producer
  extends Actor
  with ActorLogging {

  override def receive: Receive = {
    case Produce(_, queueName, interval) =>
      produce(queueName, interval)
    case r: Rejected =>
      log.info(s"Job [${r.id}] REJECTED :(")
    case _ => // TODO
  }

  def produce(queueName: String, interval: FiniteDuration) {
    val id = UUID.randomUUID().toString
    log.info(s"[$id] Job PRODUCED")
    sender() ! DeliverJob(id, queueName)

    Thread.sleep(interval.toMillis)

    produce(queueName, interval)
  }

}
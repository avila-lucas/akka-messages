package com.omnipresent.model

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.cluster.sharding.{ ClusterSharding, ShardRegion }
import com.omnipresent.model.Consumer.ConsumedJob
import com.omnipresent.model.MessagesQueue.{ ProxyJob, Start }
import com.omnipresent.model.MessagesQueueProxy.FailedReception
import com.omnipresent.model.Producer.{ DeliverJob, Produce }
import com.omnipresent.system.Master.CreateProducer
import org.apache.commons.lang3.time.StopWatch

import scala.concurrent.duration.FiniteDuration

object MessagesQueue {

  final case class Start(queueName: String, producers: Int, workers: Int, interval: FiniteDuration)

  final case class ProxyJob(jobId: String, deliveryId: Long, watch: StopWatch, transactional: Boolean)

  def props(spreadType: String): Props = Props(new MessagesQueue(spreadType))

  case class GetQueue(queueName: String)

  val entityIdExtractor: ShardRegion.ExtractEntityId = {
    case s: Start => (s.queueName, s)
    case d: DeliverJob => (d.queueName, d)
    case g: GetQueue => (g.queueName, g)
  }

  val shardIdExtractor: ShardRegion.ExtractShardId = {
    case s: Start => (math.abs(s.queueName.split("_").last.toLong.hashCode) % 100).toString
    case d: DeliverJob => (math.abs(d.queueName.hashCode) % 100).toString
    case GetQueue(queueName) => (math.abs(queueName.hashCode) % 100).toString
  }

  val pubSubShardName: String = "Queues-PubSub"
  val broadcastShardName: String = "Queues-Broadcast"
}

class MessagesQueue(spreadType: String)
  extends Actor
  with ActorLogging {

  val queueId = s"queue_${UUID.randomUUID().getMostSignificantBits}"
  var proxyQueue: Option[ActorRef] = None

  private val producerRegion: ActorRef = ClusterSharding(context.system).shardRegion(Producer.shardName)

  override def receive: Receive = {

    case CreateProducer(_, interval, _) =>
      producerRegion ! Produce(s"producer_${UUID.randomUUID().getMostSignificantBits}", queueId, FiniteDuration(interval.toLong, TimeUnit.SECONDS))

    case Start(queueName, producers, workers, interval) =>
      log.info(s"Starting Queue [$queueId]")
      1 to producers foreach {
        idx =>
          producerRegion ! Produce(s"producer-${idx}_${UUID.randomUUID().getMostSignificantBits}", queueName, interval)
      }
      log.info(s"[$producers] producers  were created for [$queueId]")

      proxyQueue = Some(context.actorOf(Props(new MessagesQueueProxy(queueName, spreadType, workers)), s"${queueName}_proxy"))
      log.info("Proxy queue created")
      log.info(s"Finished creating queue [$queueName]")

    case job: DeliverJob =>
      log.info(s"[${job.id}] RECEIVED (queue)")
      proxyQueue.foreach(p => p ! ProxyJob(job.id, UUID.randomUUID().getMostSignificantBits, StopWatch.createStarted(), true))

    case ConsumedJob(jobId, _) =>
      log.info(s"[$jobId] CONFIRMED")

    case FailedReception(_) =>
      log.info(s"FAILED JOB")

  }

}

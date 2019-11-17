package com.omnipresent.model

import java.util.concurrent.TimeUnit

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.cluster.sharding.{ ClusterSharding, ShardRegion }
import com.omnipresent.model.Consumer.ConsumedJob
import com.omnipresent.model.MessagesQueue.{ OnGoingJob, ProxyJob, RetryJob, Start }
import com.omnipresent.model.MessagesQueueProxy.FailedReception
import com.omnipresent.model.Producer.{ DeliverJob, Produce }
import com.omnipresent.support.IdGenerator
import com.omnipresent.system.Master.CreateProducer
import org.apache.commons.lang3.time.StopWatch

import scala.concurrent.duration.FiniteDuration

object MessagesQueue {

  final case class Start(queueName: String, producers: Int, workers: Int, interval: FiniteDuration)

  final case class ProxyJob(jobId: String)

  final case class RetryJob(job: ProxyJob, retryWorkers: List[String])

  final case class OnGoingJob(job: ProxyJob, watch: StopWatch)

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

  private val producerRegion: ActorRef = ClusterSharding(context.system).shardRegion(Producer.shardName)
  val queueId: String = IdGenerator.getRandomID("queue")
  var onGoingJobs: Map[String, OnGoingJob] = Map.empty
  var proxyQueue: Option[ActorRef] = None

  override def receive: Receive = {

    case CreateProducer(_, interval, _) =>
      producerRegion ! Produce(IdGenerator.getRandomID("producer"), queueId, FiniteDuration(interval.toLong, TimeUnit.SECONDS))

    case Start(queueName, producers, workers, interval) =>
      log.info(s"Starting Queue [$queueId]")
      1 to producers foreach {
        idx =>
          producerRegion ! Produce(IdGenerator.getRandomID(s"${queueName}-PRODUCER-${idx}"), queueName, interval)
      }
      log.info(s"[$producers] producers  were created for [$queueId]")

      proxyQueue = Some(context.actorOf(Props(new MessagesQueueProxy(queueName, spreadType, workers)), s"${queueName}_PROXY"))
      log.info("Proxy queue created")
      log.info(s"Finished creating queue [$queueName]")

    case job: DeliverJob =>
      val newJob = ProxyJob(job.id)
      onGoingJobs = onGoingJobs.updated(job.id, OnGoingJob(newJob, StopWatch.createStarted()))
      log.info(s"[${job.id}] RECEIVED (queue)")

      proxyQueue.foreach(p => p ! newJob)

    case ConsumedJob(jobId) =>
      onGoingJobs.get(jobId).map {
        onGoingJob =>
          log.info(s"[$jobId] CONFIRMED in [${onGoingJob.watch.getTime(TimeUnit.SECONDS)}] seconds")
          onGoingJobs = onGoingJobs - jobId
      }
    case FailedReception(job, retryWorkers) =>
      log.info(s"[${job.jobId}] FAILED JOB, retrying...")
      proxyQueue.map(_ ! RetryJob(job, retryWorkers))
  }

}

package com.omnipresent.model

import java.util.concurrent.TimeUnit

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata.{ DistributedData, ORSet, ORSetKey, SelfUniqueAddress }
import akka.cluster.sharding.{ ClusterSharding, ShardRegion }
import com.omnipresent.model.Consumer.ConsumedJob
import com.omnipresent.model.MessagesQueue._
import com.omnipresent.model.MessagesQueueProxy.FailedReception
import com.omnipresent.model.Producer.{ DeliverJob, Produce }
import com.omnipresent.support.IdGenerator
import com.omnipresent.system.Master.{ CheckQueues, CreateProducer }
import org.apache.commons.lang3.time.StopWatch

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object MessagesQueue {

  final case class HeartBeat(queueName: String)

  final case class Start(queueName: String, producers: Int, workers: Int, interval: FiniteDuration)

  final case class ProxyJob(jobId: String)

  final case class AddJob(jobId: String)

  final case class RemoveJob(jobId: String)

  final case class RetryJob(job: ProxyJob, retryWorkers: List[String])

  final case class OnGoingJob(job: ProxyJob, watch: StopWatch)

  def props(spreadType: String): Props = Props(new MessagesQueue(spreadType))

  case class GetQueue(queueName: String)

  val entityIdExtractor: ShardRegion.ExtractEntityId = {
    case s: Start => (s.queueName, s)
    case h: HeartBeat => (h.queueName, h)
    case d: DeliverJob => (d.queueName, d)
    case g: GetQueue => (g.queueName, g)
  }

  val shardIdExtractor: ShardRegion.ExtractShardId = {
    case s: Start => (math.abs(s.queueName.split("_").last.toLong.hashCode) % 100).toString
    case h: HeartBeat => (math.abs(h.queueName.split("_").last.toLong.hashCode) % 100).toString
    case d: DeliverJob => (math.abs(d.queueName.hashCode) % 100).toString
    case GetQueue(queueName) => (math.abs(queueName.hashCode) % 100).toString
    case ShardRegion.StartEntity(id) ⇒
      (math.abs(id.split("_").last.toLong.hashCode) % 100).toString
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

  implicit val ec: ExecutionContext = context.dispatcher
  implicit val node: SelfUniqueAddress = DistributedData(context.system).selfUniqueAddress
  val replicator: ActorRef = DistributedData(context.system).replicator

  val OnGoingJobsKey: ORSetKey[String] = ORSetKey[String]("queueId")
  /* Weird thing happened here...if you use the queueId with a random name the Get state fails...
      now every queue is using the same job queue.
   */
  val readMajority = ReadMajority(timeout = 1.seconds)
  val writeMajority = WriteMajority(timeout = 5.seconds)

  override def receive: Receive = {

    case HeartBeat(_) =>
      log.info("I'm UPPPPPPPPP")
      replicator ! Get(OnGoingJobsKey, readMajority, request = Some(sender(), "jobs"))

    case g @ GetSuccess(OnGoingJobsKey, Some((replyTo: ActorRef, "jobs"))) =>
      log.info("Printing out jobs")
      val value = g.get(OnGoingJobsKey).elements
      log.info(s"Pending jobs: $value")

    case CreateProducer(_, interval, _) =>
      producerRegion ! Produce(IdGenerator.getRandomID("producer"), queueId, FiniteDuration(interval.toLong, TimeUnit.SECONDS))

      // FIXME - What happenes if the queue dies? Hard to know
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
      val req = Some(AddJob(job.id))
      replicator ! Update(OnGoingJobsKey, ORSet.empty[String], writeMajority, req)(_ :+ job.id)
      //onGoingJobs = onGoingJobs.updated(job.id, OnGoingJob(newJob, StopWatch.createStarted()))
      log.info(s"[${job.id}] RECEIVED (queue)")
      proxyQueue.foreach(p => p ! newJob)

    case UpdateSuccess(_, Some(request: AddJob)) =>
      log.info(s"Successfully added job ${request.jobId}")

    case UpdateTimeout(_, Some(request: AddJob)) =>
      log.error(s"Fail to add new job ${request.jobId}") // TODO handle error case

    case UpdateSuccess(_, Some(request: RemoveJob)) =>
      log.info(s"Successfully CONFIRMED job ${request.jobId}")

    case UpdateTimeout(_, Some(request: RemoveJob)) =>
      log.error(s"Fail to remove job ${request.jobId}") // TODO handle error case

    case ConsumedJob(jobId) =>
      val removeJob = Some(RemoveJob(jobId))
      replicator ! Update(OnGoingJobsKey, ORSet.empty[String], writeMajority, removeJob)(_.remove(jobId))

    case FailedReception(job, retryWorkers) =>
      log.info(s"[${job.jobId}] FAILED JOB, retrying...")
      proxyQueue.map(_ ! RetryJob(job, retryWorkers))

    case e =>
      log.info(s"Missing messageeeeee ${e}")
  }

}

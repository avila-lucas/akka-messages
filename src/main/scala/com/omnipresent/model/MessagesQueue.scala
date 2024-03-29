package com.omnipresent.model

import java.math.BigInteger
import java.time.{ Instant, LocalDateTime, ZoneOffset }
import java.util.concurrent.TimeUnit

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata.{ DistributedData, FlagKey, ORMapKey, ORMultiMap, ORSet, ORSetKey, PNCounter, PNCounterKey, SelfUniqueAddress }
import akka.cluster.sharding.{ ClusterSharding, ShardRegion }
import com.omnipresent.model.MessagesQueue._
import com.omnipresent.model.Producer.{ DeliverJob, Produce }
import com.omnipresent.support.IdGenerator
import com.omnipresent.system.Master.CreateProducer
import org.apache.commons.lang3.time.StopWatch

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object MessagesQueue {

  final case class PreStart(queueName: Option[String], workers: Option[Int], producers: Option[Int])

  final case class HeartBeat(queueName: String)

  final case class Start(queueName: String, producers: Int, workers: Int, interval: FiniteDuration)

  final case class QueueConsumedJob(queueName: String, jobId: String)

  final case class ProxyJob(jobId: String)

  final case class AddJob(jobId: String)

  final case class RemoveJob(jobId: String)

  final case class RetryJob(job: ProxyJob)

  final case class OnGoingJob(job: ProxyJob, watch: StopWatch)

  final case class JobTimeout(queueName: String, jobId: String)

  def props(spreadType: String): Props = Props(new MessagesQueue(spreadType))

  case class GetQueue(queueName: String)

  val entityIdExtractor: ShardRegion.ExtractEntityId = {
    case s: Start => (s.queueName, s)
    case h: HeartBeat => (h.queueName, h)
    case d: DeliverJob => (d.queueName, d)
    case g: GetQueue => (g.queueName, g)
    case q: QueueConsumedJob => (q.queueName, q)
    case f: JobTimeout => (f.queueName, f)
  }

  val shardIdExtractor: ShardRegion.ExtractShardId = {
    case s: Start => (math.abs(s.queueName.hashCode) % 100).toString
    case h: HeartBeat => (math.abs(h.queueName.hashCode) % 100).toString
    case d: DeliverJob => (math.abs(d.queueName.hashCode) % 100).toString
    case g: GetQueue => (math.abs(g.queueName.hashCode) % 100).toString
    case q: QueueConsumedJob => (math.abs(q.queueName.hashCode) % 100).toString
    case f: JobTimeout => (math.abs(f.queueName.hashCode) % 100).toString
    case ShardRegion.StartEntity(id) ⇒
      (math.abs(id.split("_").last.toLong.hashCode) % 100).toString
  }

  val pubSubShardName: String = "Queues-pubsub"
  val broadcastShardName: String = "Queues-broadcast"
}

class MessagesQueue(spreadType: String)
  extends Actor
  with ActorLogging {

  private val producerRegion: ActorRef = ClusterSharding(context.system).shardRegion(Producer.shardName)
  val queueId: String = IdGenerator.getRandomID("queue")
  var proxyQueue: Option[ActorRef] = None

  implicit val ec: ExecutionContext = context.dispatcher
  implicit val node: SelfUniqueAddress = DistributedData(context.system).selfUniqueAddress
  val replicator: ActorRef = DistributedData(context.system).replicator

  val OnGoingJobsKey: ORSetKey[String] = ORSetKey[String](s"${self.path.name}-ongoingJobs")
  val QueueNameKey: ORSetKey[String] = ORSetKey[String](s"${self.path.name}-queueName")
  val QueueProducersKey: PNCounterKey = PNCounterKey(s"${self.path.name}-producers")
  val QueueWorkers: PNCounterKey = PNCounterKey(s"${self.path.name}-workers")
  val readMajority = ReadMajority(timeout = 1.seconds)
  val writeMajority = WriteMajority(timeout = 5.seconds)

  val jobExpireTime = 100

  override def preStart(): Unit = {
    log.info(s"PRE START ${self.path.name}")
    replicator ! Get(QueueNameKey, readMajority, request = None)
  }

  override def receive: Receive = {

    case g @ GetSuccess(QueueNameKey, _) =>
      log.info("Pre start queue name")
      val queueName = g.get(QueueNameKey).elements.head
      replicator ! Get(QueueWorkers, readMajority, request = Some(PreStart(Some(queueName), None, None)))

    case g @ GetSuccess(QueueWorkers, Some(PreStart(Some(queueName), _, _))) =>
      log.info("Pre start workers")
      val workers = g.get(QueueWorkers).getValue.intValue()
      val proxy = context.actorOf(Props(new MessagesQueueProxy(queueName, spreadType, workers)), s"${queueName}_PROXY")
      log.info(s"Proxy queue actor ${proxy}")
      proxyQueue = Some(proxy)
      replicator ! Get(QueueProducersKey, readMajority, request = Some(PreStart(Some(queueName), Some(workers), None)))

    case g @ GetSuccess(QueueProducersKey, Some(PreStart(Some(queueName), _, _))) =>
      val producers = g.get(QueueProducersKey).getValue.intValue()
      1 to producers foreach {
        idx =>
          producerRegion ! Produce(s"${queueName}-PRODUCER-${idx}", queueName, spreadType, interval = 5.seconds)
      }
      log.info(s"[${producers}] producers  were created for [$queueId]")

    case HeartBeat(_) =>
      log.info("HEARTBEAT OK")
      replicator ! Get(OnGoingJobsKey, readMajority, request = Some(sender(), "jobs"))

    case g @ GetSuccess(OnGoingJobsKey, Some((_: ActorRef, "jobs"))) =>
      log.info("Printing out jobs")
      val jobs = g.get(OnGoingJobsKey).elements
      log.info(s"Pending jobs: $jobs")
      val queueRegion: ActorRef = ClusterSharding(context.system).shardRegion(s"Queues-${spreadType}")
      jobs.collect {
        case job if LocalDateTime.ofInstant(Instant.ofEpochMilli(job.split('-').last.toLong), ZoneOffset.UTC).plusSeconds(jobExpireTime).isBefore(LocalDateTime.now()) =>
          queueRegion ! JobTimeout(self.path.name, job)
      }

    case CreateProducer(queueName, interval, _) =>
      producerRegion ! Produce(IdGenerator.getRandomID("producer"), queueName, spreadType, FiniteDuration(interval.toLong, TimeUnit.SECONDS))

    case s @ Start(queueName, producers, workers, interval) =>
      log.info(s"Starting Queue [$queueId]")
      val request = PreStart(Some(queueName), Some(workers), Some(producers))
      replicator ! Update(QueueNameKey, ORSet.empty[String], writeMajority, Some(request))(_ :+ queueName)
      log.info(s"Finished creating queue [$queueName]")

    case job: DeliverJob =>
      val newJob = ProxyJob(job.id)
      val req = Some(AddJob(job.id))
      replicator ! Update(OnGoingJobsKey, ORSet.empty[String], writeMajority, req)(_ :+ job.id)
      log.info(s"Proxy queue: ${proxyQueue}")
      proxyQueue.foreach(p => p ! newJob)
      log.info(s"[${job.id}] RECEIVED (queue)")

    case UpdateSuccess(QueueNameKey, Some(request: PreStart)) =>
      log.info(s"Successfully update queue name")
      val workers = request.workers.getOrElse(0)
      replicator ! Update(QueueWorkers, PNCounter.empty, writeMajority, Some(request))(_ :+ workers.toLong)

    case UpdateSuccess(QueueWorkers, Some(request: PreStart)) =>
      log.info(s"Successfully update workers")
      val producers = request.producers.getOrElse(0)
      replicator ! Update(QueueProducersKey, PNCounter.empty, writeMajority, Some(request))(_ :+ producers.toLong)

    case UpdateSuccess(QueueProducersKey, Some(p: PreStart)) =>
      log.info(s"Successfully update producers")
      preStart()

    case UpdateSuccess(_, Some(request: AddJob)) =>
      log.info(s"Successfully added job ${request.jobId}")

    case UpdateTimeout(_, Some(request: AddJob)) =>
      log.error(s"Fail to add new job ${request.jobId}") // TODO handle error case

    case UpdateSuccess(_, Some(request: RemoveJob)) =>
      log.info(s"Successfully CONFIRMED job ${request.jobId}")

    case UpdateTimeout(_, Some(request: RemoveJob)) =>
      log.error(s"Fail to remove job ${request.jobId}") // TODO handle error case

    case QueueConsumedJob(_, jobId) =>
      val removeJob = Some(RemoveJob(jobId))
      replicator ! Update(OnGoingJobsKey, ORSet.empty[String], writeMajority, removeJob)(_.remove(jobId))

    case JobTimeout(_, job) =>
      log.info(s"[${job}] FAILED JOB, retrying...")
      proxyQueue.map(_ ! RetryJob(ProxyJob(job)))

    case e =>
      log.info(s"MISSING MESSAGE ${e}")
  }

}

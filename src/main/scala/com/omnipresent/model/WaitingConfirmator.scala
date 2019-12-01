package com.omnipresent.model

import akka.actor.{ Actor, ActorLogging, _ }
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata.{ DistributedData, ORSet, ORSetKey, SelfUniqueAddress }
import akka.cluster.sharding.{ ClusterSharding, ShardRegion }
import com.omnipresent.model.Consumer.{ ConsumedJob, Job }
import com.omnipresent.model.MessagesQueue.{ ProxyJob, QueueConsumedJob }
import com.omnipresent.model.Producer.DeliverJob
import com.omnipresent.model.WaitingConfirmator.{ ConsumedTemporalState, RetryConfirmator, RetryTemporalState, StartWaitingConfirmator }

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

object WaitingConfirmator {
  case class ConsumedTemporalState(job: ConsumedJob, queueName: Option[String], consumers: List[String])
  case class StartWaitingConfirmator(consumers: List[String], job: ProxyJob, queueName: String, queueRegion: ActorRef)

  case class RetryConfirmator(queueName: String, job: ProxyJob)
  case class RetryTemporalState(retry: RetryConfirmator, confirmedJobs: Option[List[String]])

  def props(spreadType: String): Props = Props(new WaitingConfirmator(spreadType))

  val entityIdExtractor: ShardRegion.ExtractEntityId = {
    case c: ConsumedJob => (c.jobId, c)
    case r: RetryConfirmator => (r.job.jobId, r)
    case s: StartWaitingConfirmator => (s.job.jobId, s)
  }

  val shardIdExtractor: ShardRegion.ExtractShardId = {
    case c: ConsumedJob => (math.abs(c.jobId.hashCode) % 100).toString
    case r: RetryConfirmator => (math.abs(r.job.jobId.hashCode) % 100).toString
    case s: StartWaitingConfirmator => (math.abs(s.job.jobId.hashCode) % 100).toString
    case ShardRegion.StartEntity(id) ⇒ (math.abs(id.hashCode) % 100).toString
  }

  val pubSubShardName: String = "WaitingConfirmator-pubsub"
  val broadcastShardName: String = "WaitingConfirmator-broadcast"
}

class WaitingConfirmator(spreadType: String)
  extends Actor
  with ActorLogging {

  implicit val ex: ExecutionContextExecutor = context.system.dispatcher
  implicit val node: SelfUniqueAddress = DistributedData(context.system).selfUniqueAddress
  val replicator: ActorRef = DistributedData(context.system).replicator

  val ConsumersKey: ORSetKey[String] = ORSetKey[String](s"${self.path.name}-consumers")
  val QueueNameKey: ORSetKey[String] = ORSetKey[String](s"${self.path.name}-queueName")
  val ConfirmedJobsKey: ORSetKey[String] = ORSetKey[String](s"${self.path.name}-confirmedJobs")
  val readMajority = ReadMajority(timeout = 1.seconds)
  val writeMajority = WriteMajority(timeout = 5.seconds)

  private val queueRegion: ActorRef = ClusterSharding(context.system).shardRegion(s"Queues-${spreadType}")
  private val consumerRegion: ActorRef = ClusterSharding(context.system).shardRegion(Consumer.transactionalShardName) // FIXME change shard by consumer

  override def receive: Receive = {

    case consumed: ConsumedJob =>
      log.info(s"[${consumed.jobId}] RECEIVED (waiting confirmator)")
      replicator ! Get(QueueNameKey, readMajority, request = Some(ConsumedTemporalState(consumed, None, List.empty)))

    case g @ GetSuccess(QueueNameKey, Some(ConsumedTemporalState(consumed, _, _))) =>
      log.info("Successfully get queue name")
      val queueName = g.get(QueueNameKey).elements.headOption
      replicator ! Get(ConsumersKey, readMajority, request = Some(ConsumedTemporalState(consumed, queueName, List.empty)))

    case g @ GetSuccess(ConsumersKey, Some(ConsumedTemporalState(job, queueName, _))) =>
      log.info("Successfully get consumers")
      val consumers = g.get(ConsumersKey).elements
      replicator ! Update(ConfirmedJobsKey, ORSet.empty[String], writeMajority, Some(ConsumedTemporalState(job, queueName, consumers.toList)))(_ :+ job.jobId)

    case g @ UpdateSuccess(ConfirmedJobsKey, Some(ConsumedTemporalState(job, queueName, consumers))) =>
      log.info("Successfully update confirmedJobs")
      replicator ! Get(ConfirmedJobsKey, readMajority, request = Some(ConsumedTemporalState(job, queueName, consumers)))

    case g @ GetSuccess(ConfirmedJobsKey, Some(ConsumedTemporalState(job, queueName, consumers))) =>
      log.info("Successfully get confirmedJobs")
      val confirmedJobs = g.get(ConfirmedJobsKey).elements
      log.info(s"[${job.jobId}] ACK")
      answerIfDone(job, consumers, confirmedJobs, queueName.getOrElse(""))

    case start: StartWaitingConfirmator =>
      replicator ! Update(QueueNameKey, ORSet.empty[String], writeMajority, Some(start))(_ :+ start.queueName)

    case UpdateSuccess(QueueNameKey, Some(start: StartWaitingConfirmator)) =>
      log.info(s"Successfully update queue name")
      replicator ! Update(ConsumersKey, ORSet.empty[String], writeMajority, Some(start))(set => {
        start.consumers.foldLeft(set)((set, c) => set :+ c)
      })

    case UpdateSuccess(ConsumersKey, Some(start: StartWaitingConfirmator)) =>
      log.info(s"Successfully update consumers")
      initConsumers(start)

    case retry: RetryConfirmator =>
      replicator ! Get(ConfirmedJobsKey, readMajority, request = Some(RetryTemporalState(retry, None)))

    case g @ GetSuccess(ConfirmedJobsKey, Some(RetryTemporalState(retry, _))) =>
      log.info("Successfully get confirmedJobs")
      val confirmedJobs = g.get(ConfirmedJobsKey).elements.toList
      replicator ! Get(ConsumersKey, readMajority, request = Some(RetryTemporalState(retry, Some(confirmedJobs))))

    case NotFound(ConfirmedJobsKey, Some(RetryTemporalState(retry, _))) =>
      log.info("Empty confirmedJobs")
      val confirmedJobs = Set.empty[String].toList
      replicator ! Get(ConsumersKey, readMajority, request = Some(RetryTemporalState(retry, Some(confirmedJobs))))

    case NotFound(ConsumersKey, Some(RetryTemporalState(retry, _))) =>
      log.info("Waiting confirmator bad formed, starting over...")
      queueRegion ! DeliverJob(retry.job.jobId, retry.queueName)
      log.info("Suicide...")
      context stop self

    case g @ GetSuccess(ConsumersKey, Some(RetryTemporalState(retry, Some(confirmedJobs)))) =>
      log.info("Successfully get consumers")
      val consumers = g.get(ConsumersKey).elements.toList.diff(confirmedJobs)
      createConsumers(consumers, retry.job.jobId)

    case e => log.info(s"MISSING MESSAGE ${e}")

  }

  private def initConsumers(start: StartWaitingConfirmator): Unit = {
    createConsumers(start.consumers, start.job.jobId)
  }

  private def createConsumers(consumers: List[String], jobId: String): Unit = {
    consumers map {
      name =>
        consumerRegion ! Job(consumerName = name, jobId, waitingShardName = s"WaitingConfirmator-${spreadType}")
    }

  }

  private def answerIfDone(consumedJob: ConsumedJob, consumers: List[String], confirmedJobs: Set[String], queueName: String): Unit = {
    if (confirmedJobs.size == consumers.size) acknowledge(consumedJob, queueName)
  }

  private def acknowledge(consumedJob: ConsumedJob, queueName: String) {
    log.info(s"[${consumedJob.jobId}] ACKNOWLEDGE")
    queueRegion ! QueueConsumedJob(queueName, consumedJob.jobId)
    context stop self
  }
}

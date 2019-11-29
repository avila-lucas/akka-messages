package com.omnipresent.system

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props}
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata.{DistributedData, ORSet, ORSetKey, SelfUniqueAddress}
import akka.cluster.sharding.ClusterSharding
import com.omnipresent.model.MessagesQueue
import com.omnipresent.model.MessagesQueue.{HeartBeat, Start}
import com.omnipresent.support.IdGenerator
import com.omnipresent.system.Master._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

final case class ProducerCreationResult(created: Boolean)

final case class QueuesNames(queues: Set[String])

object Master {

  def props: Props = Props(new Master)

  case object CheckQueues

  final case class ActionPerformed(description: String)

  final case class CreateQueueRequest(replyTo: ActorRef, queueName: String)

  final case object GetQueuesNames

  final case class GetQueue(name: String)

  final case class CreateQueue(name: String, producers: Int, workers: Int, jobInterval: Long, spreadType: String = "RR")

  final case class CreateProducer(queueName: String, interval: Long, transactional: Boolean)

}

class Master
  extends Actor
    with ActorLogging {

  private val broadcastRegion: ActorRef = ClusterSharding(context.system).shardRegion(MessagesQueue.broadcastShardName)

  private val pubSubRegion: ActorRef = ClusterSharding(context.system).shardRegion(MessagesQueue.pubSubShardName)

  implicit val ec: ExecutionContext = context.dispatcher

  val replicator = DistributedData(context.system).replicator

  implicit val node: SelfUniqueAddress = DistributedData(context.system).selfUniqueAddress

  val timeoutScheduled: Cancellable = context.system.scheduler.schedule(10.seconds, 10.seconds, self, CheckQueues)

  val QueueDataKey: ORSetKey[String] = ORSetKey("queues")
  val readMajority = ReadMajority(timeout = 1.seconds)
  val writeMajority = WriteMajority(timeout = 5.seconds)

  override def preStart(): Unit = log.info("Akka Messages Master application started")

  override def postStop(): Unit = log.info("Akka Messages Master application stopped")

  override def receive: Receive = {

    case createProducer: CreateProducer =>
      sender() ! ProducerCreationResult(false)

    case GetQueuesNames =>
      log.info("Collecting queues names from majority")
      replicator ! Get(QueueDataKey, readMajority, request = Some(sender()))

    case CheckQueues =>
      log.info("Time to check QUEUES!")
      replicator ! Get(QueueDataKey, readMajority, request = Some(sender(), CheckQueues))

    case details: CreateQueue =>
      log.info(s"Request to CREATE QUEUE: ${details.name}")
      val name = createQueue(details)
      val request = Some(CreateQueueRequest(sender(), name))
      replicator ! Update(QueueDataKey, ORSet.empty[String], writeMajority, request = request)(_ :+ name)

    case g@GetSuccess(QueueDataKey, Some((_: ActorRef, CheckQueues))) =>
      val value = g.get(QueueDataKey).elements
      log.info(s"Checking queues: $value")
      value.foreach(name => broadcastRegion ! HeartBeat(name))

    case g@GetSuccess(QueueDataKey, Some(replyTo: ActorRef)) =>
      val value = g.get(QueueDataKey).elements
      replyTo ! QueuesNames(value)

    case NotFound(QueueDataKey, Some(replyTo: ActorRef)) =>
      replyTo ! QueuesNames(Set.empty)

    case GetFailure(QueueDataKey, Some(_: ActorRef)) =>
      // ReadMajority failure, try again with local read
      log.error("Fail to get queue names from majority attempt to get from local")

    case UpdateSuccess(QueueDataKey, Some(request: CreateQueueRequest)) =>
      request.replyTo ! ActionPerformed(s"Queue [${request.queueName}] created")

    case UpdateTimeout(QueueDataKey, Some(request: CreateQueueRequest)) =>
      log.error(s"Fail to create new queue ${request.queueName}") // TODO handle error case

    case _ =>
      log.warning("MISSED MESSAGE?")
  }

  private def createQueue(details: CreateQueue): String = {
    val name = IdGenerator.getRandomID(details.name)
    details match {
      case CreateQueue(name, producers, workers, interval, spreadType) if spreadType.equalsIgnoreCase("pubsub") =>
        pubSubRegion ! Start(name, producers, workers, FiniteDuration(interval.toLong, TimeUnit.SECONDS))
      case CreateQueue(name, producers, workers, interval, _) =>
        broadcastRegion ! Start(name, producers, workers, FiniteDuration(interval.toLong, TimeUnit.SECONDS))
    }
    name
  }

}

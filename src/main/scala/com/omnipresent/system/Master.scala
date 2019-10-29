package com.omnipresent.system

import java.util.concurrent.TimeUnit

import akka.actor.{ActorLogging, ActorRef, Props}
import akka.persistence.{PersistentActor, RecoveryCompleted}
import com.omnipresent.model.MessagesQueue.Start
import com.omnipresent.system.Master.{ActionPerformed, CreateProducer, CreateQueue, GetQueuesNames}
import com.omnipresent.system.QueueSystemState.{AddNewQueue, QueueSystemEvent}

import scala.concurrent.duration.FiniteDuration

final case class ProducerCreationResult(created: Boolean)

final case class QueuesNames(queues: Set[String])

object Master {

  def props(broadcast: ActorRef, pubSub: ActorRef): Props = Props(new Master(broadcast, pubSub))

  final case class ActionPerformed(description: String)

  final case object GetQueuesNames

  final case class GetQueue(name: String)

  final case class CreateQueue(name: String, producers: Int, workers: Int, jobInterval: Long, spreadType: String = "RR")

  final case class CreateProducer(queueName: String, interval: Long, transactional: Boolean)

}

class Master(broadcastRegion: ActorRef, pubSubRegion: ActorRef)
  extends PersistentActor
    with ActorLogging {

  override val persistenceId: String = "master"

  private var queueSystemState = QueueSystemState.empty

  override def preStart(): Unit = log.info("Akka Messages Master application started")

  override def postStop(): Unit = log.info("Akka Messages Master application stopped")

  override def receiveRecover: Receive = {

    case event: QueueSystemEvent =>
      queueSystemState = queueSystemState.updated(event)
      log.info("Replayed {}", event.getClass.getSimpleName)

    case RecoveryCompleted =>
      log.info("Recovery completed")

  }

  override def receiveCommand: Receive = {

    case createProducer: CreateProducer =>
      //      val result = queueSystemState
      //        .find(createProducer.queueName)
      //        .map(queue => queue ! createProducer)
      //        .isDefined
      sender() ! ProducerCreationResult(false)

    case GetQueuesNames =>
      log.info("Collecting queues names")
      sender() ! QueuesNames(queueSystemState.queueNames)

    case details: CreateQueue =>
      log.info(s"Request to CREATE QUEUE: ${details.name}")
      persist(AddNewQueue(details.name)) { event ⇒
        createQueue(details)
        sender() ! ActionPerformed(s"Queue [${details.name}] created")
        queueSystemState = queueSystemState.updated(event)
      }
  }

  private def createQueue(details: CreateQueue) =
    details match {
      case CreateQueue(name, producers, workers, interval, spreadType) if spreadType.equalsIgnoreCase("pubsub") =>
        pubSubRegion ! Start(name, producers, workers, FiniteDuration(interval.toLong, TimeUnit.SECONDS))
      case CreateQueue(name, producers, workers, interval, _) =>
        broadcastRegion ! Start(name, producers, workers, FiniteDuration(interval.toLong, TimeUnit.SECONDS))
    }

}

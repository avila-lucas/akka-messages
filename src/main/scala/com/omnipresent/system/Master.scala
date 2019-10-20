package com.omnipresent.system

import java.util.concurrent.TimeUnit

import akka.actor.{ActorLogging, ActorRef, Props}
import akka.persistence.{PersistentActor, RecoveryCompleted}
import com.omnipresent.model.MessagesQueueProxy.Start
import com.omnipresent.model.{MessagesQueue, MessagesQueueProxy}
import com.omnipresent.system.Master.{ActionPerformed, CreateProducer, CreateQueue, GetQueuesNames}
import com.omnipresent.system.QueueSystemState.{AddNewQueue, QueueSystemEvent}

import scala.concurrent.duration.FiniteDuration

final case class ProducerCreationResult(created: Boolean)

final case class QueuesNames(queues: Set[String])

object Master {

  def props(): Props = Props(new Master)

  final case class ActionPerformed(description: String)

  final case object GetQueuesNames

  final case class GetQueue(name: String)

  final case class CreateQueue(name: String, producers: Int, workers: Int, jobInterval: Long, spreadType: String = "RR")

  final case class CreateProducer(queueName: String, interval: Long, transactional: Boolean)

}

class Master
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
      val result = queueSystemState
        .find(createProducer.queueName)
        .map(queue => queue ! createProducer)
        .isDefined
      sender() ! ProducerCreationResult(result)

    case GetQueuesNames =>
      log.info("Collecting queues names")
      sender() ! QueuesNames(queueSystemState.queueNames)

    case details: CreateQueue =>
      log.info(s"Request to CREATE QUEUE: ${details.name}")
      persist(AddNewQueue(details.name, createQueue(details))) { event ⇒
        sender() ! ActionPerformed(s"Queue [${details.name}] created")
        queueSystemState = queueSystemState.updated(event)
      }
  }

  private def createQueue(details: CreateQueue): ActorRef = {
    details match {
      case CreateQueue(name, producers, workers, interval, spreadType) =>
        val proxyQueue = context.actorOf(Props(new MessagesQueueProxy(spreadType, workers)), s"${name}_proxy")
        val queue = context.actorOf(Props(new MessagesQueue(producers, context.actorSelection(proxyQueue.path))), name)
        queue ! Start(FiniteDuration(interval.toLong, TimeUnit.SECONDS))
        queue
    }
  }

}
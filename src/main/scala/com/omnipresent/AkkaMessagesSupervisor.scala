package com.omnipresent

import java.util.concurrent.TimeUnit

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import com.omnipresent.model.MessagesQueueProxy.Start
import com.omnipresent.model.{ MessagesQueue, MessagesQueueProxy, Producer }

import scala.concurrent.duration.FiniteDuration

final case class QueuesNames(queues: Seq[String])

final case class QueueInfo(exists: Boolean)

final case class ProducerCreationResult(created: Boolean)

object AkkaMessagesSupervisor {

  final case class ActionPerformed(description: String)

  final case object GetQueuesNames

  final case class GetQueue(name: String)

  final case class CreateQueue(name: String, producers: Int, workers: Int, jobInterval: Long, spreadType: String = "RR")

  final case class CreateProducer(queueName: String, productionCapacity: Int)

  def props(): Props = Props[AkkaMessagesSupervisor]
}

class AkkaMessagesSupervisor extends Actor with ActorLogging {

  import AkkaMessagesSupervisor._

  override def preStart(): Unit = log.info("Akka Messages application started")

  override def postStop(): Unit = log.info("Akka Messages application stopped")

  var queues = Map.empty[String, ActorRef]

  def receive: Receive = {
    case GetQueuesNames =>
      log.info("Collecting queues names")
      sender() ! QueuesNames(queues.keys.toSeq)
    case GetQueue(name) =>
      sender() ! QueueInfo(queues.get(name).isDefined)
    case details: CreateQueue =>
      createQueue(details)
      sender() ! ActionPerformed(s"Queue [${details.name}] created")
    case CreateProducer(queueName, _) =>
      val result = queues
        .get(queueName)
        .map(_ => context.actorOf(Props[Producer]))
        .isDefined
      sender() ! ProducerCreationResult(result)
  }

  private def createQueue(details: CreateQueue) {
    details match {
      case CreateQueue(name, producers, workers, interval, spreadType) =>
        val proxyQueue = context.actorOf(Props(new MessagesQueueProxy(spreadType, workers)), s"${name}_proxy")
        val queue = context.actorOf(Props(new MessagesQueue(producers, context.actorSelection(proxyQueue.path))), name)
        queue ! Start(FiniteDuration(interval.toLong, TimeUnit.SECONDS))
        queues = queues.updated(name, queue)
    }
  }

}

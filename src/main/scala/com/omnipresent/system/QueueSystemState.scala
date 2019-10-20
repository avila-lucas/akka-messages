package com.omnipresent.system

import akka.actor.ActorRef
import com.omnipresent.system.QueueSystemState.{AddNewQueue, QueueSystemEvent}

object QueueSystemState {

  def empty: QueueSystemState = QueueSystemState(queues = Map.empty[String, ActorRef])

  trait QueueSystemEvent

  final case class AddNewQueue(queueName: String, queue: ActorRef) extends QueueSystemEvent

}

case class QueueSystemState private(private val queues: Map[String, ActorRef]) {

  def updated(event: QueueSystemEvent): QueueSystemState = event match {
    case AddNewQueue(queueName, queue) ⇒
      copy(queues = queues.updated(queueName, queue))
  }

  def queueNames: Set[String] = queues.keySet

  def find(name: String): Option[ActorRef] = queues.get(name)

}

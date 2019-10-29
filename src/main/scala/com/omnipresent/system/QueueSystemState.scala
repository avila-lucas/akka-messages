package com.omnipresent.system

import com.omnipresent.system.QueueSystemState.{AddNewQueue, QueueSystemEvent}

object QueueSystemState {

  def empty: QueueSystemState = QueueSystemState(queues = Set.empty[String])

  trait QueueSystemEvent

  final case class AddNewQueue(queueName: String) extends QueueSystemEvent

}

case class QueueSystemState private(private val queues: Set[String]) {

  def updated(event: QueueSystemEvent): QueueSystemState = event match {
    case AddNewQueue(queueName) ⇒
      copy(queues = queues + queueName)
  }

  def queueNames: Set[String] = queues

  def find(name: String): Option[String] = queues.find(_.equalsIgnoreCase(name))

}

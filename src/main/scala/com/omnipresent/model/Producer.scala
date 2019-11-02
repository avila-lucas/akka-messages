package com.omnipresent.model

import java.util.UUID

import akka.actor.{ Actor, ActorLogging }
import com.omnipresent.model.MessagesQueueProxy.{ Produce, Rejected }
import com.omnipresent.model.Producer.DeliverJob

import scala.concurrent.duration.FiniteDuration

object Producer {

  final case class DeliverJob(id: String, queueName: String, transactional: Boolean)

}

class Producer(relatedQueue: String, transactional: Boolean) extends Actor with ActorLogging {

  override def receive: Receive = {
    case Produce(interval) =>
      produce(interval)
    case r: Rejected =>
      log.info(s"Job [${r.id}] REJECTED :(")
    case _ => // TODO
  }

  def produce(interval: FiniteDuration) {
    val id = UUID.randomUUID().toString
    log.info(s"[$id] Job PRODUCED")
    sender() ! DeliverJob(id, relatedQueue, transactional)

    Thread.sleep(interval.toMillis)

    produce(interval)
  }

}
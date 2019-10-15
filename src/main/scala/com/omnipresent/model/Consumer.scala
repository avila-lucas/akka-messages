package com.omnipresent.model

import java.util.concurrent.TimeUnit

import akka.actor.{ Actor, ActorLogging, Props }
import org.apache.commons.lang3.time.StopWatch

object Consumer {

  final case class PerformCalculation(jobId: String, deliveryId: Long, watch: StopWatch)

  case class ConfirmReception(jobId: String, deliveryId: Long)

  def props: Props = Props[Consumer]
}

class Consumer extends Actor with ActorLogging {

  import Consumer._

  def receive: Receive = {
    case PerformCalculation(id, deliveryId, watch) =>
      log.info(s"[$id] RECEIVED (consumer)")
      sender() ! ConfirmReception(id, deliveryId)
      Thread.sleep(1000)
      log.info(s"[$id] DONE in [${watch.getTime(TimeUnit.SECONDS)}] seconds!")
    //sender() ! Finished(id) TODO: if we want some kind of transactionality then is just an if somewhere around here.
    case _ => // TODO
  }
}


package com.omnipresent.model

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.{ActorLogging, ActorSelection, Props}
import akka.persistence.{AtLeastOnceDelivery, PersistentActor}
import akka.routing.{ActorRefRoutee, BroadcastRoutingLogic, Router, RoutingLogic}
import com.omnipresent.system.Master.CreateProducer
import com.omnipresent.model.Consumer.{ConsumedJob, Job}
import com.omnipresent.model.MessagesQueueProxy.{FailedReception, Produce, Start}
import com.omnipresent.model.Producer.DeliverJob
import org.apache.commons.lang3.time.StopWatch

import scala.concurrent.duration.FiniteDuration

sealed trait Evt

case class MsgToSend(job: DeliverJob) extends Evt

case class MsgConfirmed(jobId: String, deliveryId: Long) extends Evt

case class MsgFailed(jobId: String, deliveryId: Long) extends Evt

/**
 * The "REAL" queue, this will keep track of confirmed and unconfirmed messages; With persistentActor we might
 * be able to recover the state and reDeliver the messages.
 */
class MessagesQueue(producers: Int, proxyQueue: ActorSelection)
  extends PersistentActor
    with AtLeastOnceDelivery
    with ActorLogging {

  override def persistenceId: String = s"queue-${UUID.randomUUID()}"

  var producerRouter: Router = createRouter(Props(new Producer(false)), "producer", BroadcastRoutingLogic(), producers)

  override def receiveCommand: Receive = {
    case Start(interval) =>
      producerRouter.route(Produce(interval), self)
    case CreateProducer(_, interval, transactional) =>
      val name = s"${persistenceId}_producer_${producerRouter.routees.size + 1}"
      val producer = context.actorOf(Props(new Producer(transactional)), name)
      producerRouter.addRoutee(producer)
      producer ! Produce(FiniteDuration(interval.toLong, TimeUnit.SECONDS))
    case job: DeliverJob =>
      persist(MsgToSend(job))(updateState)
    case ConsumedJob(jobId, deliveryId) =>
      persist(MsgConfirmed(jobId, deliveryId))(updateState)
    case FailedReception(job) =>
      persist(MsgFailed(job.jobId, job.deliveryId))(updateState)
  }

  override def receiveRecover: Receive = {
    case evt: Evt => updateState(evt)
  }

  def updateState(evt: Evt): Unit = evt match {
    case MsgToSend(job) =>
      log.info(s"[${job.id}] RECEIVED (queue)")
      deliver(proxyQueue)(deliveryId => Job(job.id, deliveryId, StopWatch.createStarted(), job.transactional))
    case MsgConfirmed(jobId, deliveryId) =>
      log.info(s"[$jobId] REMOVED [${confirmDelivery(deliveryId)}]")
    case MsgFailed(_, _) =>
      log.info(s"[$numberOfUnconfirmed] UNCONFIRMED JOBS")
  }

  private def createRouter(props: Props, nameType: String, routingLogic: RoutingLogic, quantity: Int) = {
    val routees = (1 to quantity).map { idx =>
      val r = context.actorOf(props, s"${persistenceId}_${nameType}_$idx")
      context.watch(r)
      ActorRefRoutee(r)
    }
    Router(routingLogic, routees)
  }
}

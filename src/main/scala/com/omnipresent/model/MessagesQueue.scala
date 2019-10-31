package com.omnipresent.model

import java.util.concurrent.TimeUnit

import akka.actor.{ ActorLogging, ActorRef, ActorSelection, Props }
import akka.cluster.sharding.ShardRegion
import akka.persistence.{ AtLeastOnceDelivery, PersistentActor }
import akka.routing.{ ActorRefRoutee, BroadcastRoutingLogic, Router, RoutingLogic }
import com.omnipresent.model.Consumer.ConsumedJob
import com.omnipresent.model.MessagesQueue.{ ProxyJob, Start }
import com.omnipresent.model.MessagesQueueProxy.{ FailedReception, Produce }
import com.omnipresent.model.Producer.DeliverJob
import com.omnipresent.system.Master.CreateProducer
import org.apache.commons.lang3.time.StopWatch

import scala.concurrent.duration.{ FiniteDuration, _ }

object MessagesQueue {

  final case class Start(queueName: String, producers: Int, workers: Int, interval: FiniteDuration)

  final case class ProxyJob(jobId: String, deliveryId: Long, watch: StopWatch, transactional: Boolean)

  def props(spreadType: String, consumerRegion: ActorRef): Props = Props(new MessagesQueue(spreadType, consumerRegion))

  case class GetQueue(queueName: String)

  val entityIdExtractor: ShardRegion.ExtractEntityId = {
    case s: Start => (s.queueName, s)
    case d: DeliverJob => (d.queueName, d)
    case g: GetQueue => (g.queueName, g)
  }

  val shardIdExtractor: ShardRegion.ExtractShardId = {
    case s: Start => (math.abs(s.queueName.split("_").last.toInt.hashCode) % 100).toString
    case d: DeliverJob => (math.abs(d.queueName.hashCode) % 100).toString
    case GetQueue(queueName) => (math.abs(queueName.hashCode) % 100).toString
  }

  val pubSubShardName: String = "Queues-PubSub"
  val broadcastShardName: String = "Queues-Broadcast"
}

class MessagesQueue(spreadType: String, consumerRegion: ActorRef)
  extends PersistentActor
  with AtLeastOnceDelivery
  with ActorLogging {

  override def persistenceId: String = "Queue-" + self.path.name

  override def redeliverInterval: FiniteDuration = 10.seconds

  override def maxUnconfirmedMessages = 100

  var proxyQueueSelection: Option[ActorSelection] = None

  override def receiveRecover: Receive = {
    case evt: Event => updateState(evt)
  }

  override def receiveCommand: Receive = {

    case start: Start =>
      log.info("Received msg start")
      persist(MsgStartQueue(start))(updateState)

    case CreateProducer(_, interval, transactional) =>
      //val name = s"${persistenceId}_producer_${producerRouter.routees.size + 1}"
      val producer = context.actorOf(Props(new Producer(persistenceId, transactional)), "test")
      producer ! Produce(FiniteDuration(interval.toLong, TimeUnit.SECONDS))

    case job: DeliverJob =>
      persist(MsgToSend(job))(updateState)

    case ConsumedJob(jobId, deliveryId) =>
      persist(MsgConfirmed(jobId, deliveryId))(updateState)

    case FailedReception(job) =>
      persist(MsgFailed(job.jobId, job.deliveryId))(updateState)

  }

  def updateState(evt: Event): Unit = evt match {

    case MsgStartQueue(Start(queueName, producers, workers, interval)) =>
      log.info("Starting")
      val router = createRouter(
        props = Props(new Producer(queueName, false)),
        nameType = "producer",
        routingLogic = BroadcastRoutingLogic(),
        quantity = producers)
      log.info("Producers were created")

      val proxyQueue = context.actorOf(Props(new MessagesQueueProxy(spreadType, workers)), s"${queueName}_proxy")
      proxyQueueSelection = Some(context.actorSelection(proxyQueue.path))
      log.info("Proxy queue created")
      router.route(Produce(interval), self)
      log.info(s"Finished creating queue [$queueName]")

    case MsgToSend(job) =>
      log.info(s"[${job.id}] RECEIVED (queue)")
      deliver(proxyQueueSelection.get)(deliveryId => ProxyJob(job.id, deliveryId, StopWatch.createStarted(), job.transactional))

    case MsgConfirmed(jobId, deliveryId) =>
      log.info(s"[$jobId] CONFIRMED [${confirmDelivery(deliveryId)}]")

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

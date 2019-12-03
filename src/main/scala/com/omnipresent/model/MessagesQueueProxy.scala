package com.omnipresent.model

import akka.actor.{ Actor, ActorLogging, Props, _ }
import akka.cluster.sharding.ClusterSharding
import akka.routing._
import com.omnipresent.model.MessagesQueue.{ ProxyJob, RetryJob }
import com.omnipresent.model.WaitingConfirmator.{ RetryConfirmator, StartWaitingConfirmator }

object MessagesQueueProxy {

  final case object TimedOut

  final case class FailedReception(queueName: String, job: ProxyJob, retryWorkers: List[String])

  final case class Rejected(id: String)

  def props: Props = Props[MessagesQueueProxy]
}

class MessagesQueueProxy(
  queueName: String,
  spreadType: String,
  workers: Int)
  extends Actor
  with ActorLogging {

  val spread: RoutingLogic = if (spreadType.equalsIgnoreCase("pubsub")) BroadcastRoutingLogic() else RoundRobinRoutingLogic()
  val queueShardName: String = if (spreadType.equalsIgnoreCase("pubsub")) MessagesQueue.pubSubShardName else MessagesQueue.broadcastShardName
  val queueRegion: ActorRef = ClusterSharding(context.system).shardRegion(queueShardName)
  val waitingRegion: ActorRef = ClusterSharding(context.system).shardRegion(s"WaitingConfirmator-${spreadType}")

  var consumerNames: List[String] = List.empty
  var nextWorkerToVisit: Int = 0 // FIXME Change this to distributed counter

  override def preStart(): Unit = {
    consumerNames = 1 to workers map {
      idx =>
        s"${queueName}-CONSUMER-${idx}"
    } toList

    log.info(s"Messages queue proxy started with [${consumerNames.size}] workers")
  }

  override def receive: Receive = {

    /** Main dispatcher job accordingly to spread type **/
    case job: ProxyJob =>
      spread match { // TODO: Improvement with Composition :)
        case _: BroadcastRoutingLogic =>
          sendJob(job, consumerNames)
        case _: RoundRobinRoutingLogic =>
          sendJob(job, List(consumerNames(nextWorkerToVisit)))
          updateNextWorkerToVisit()
      }

    /** To retry job with some specific workers **/
    case RetryJob(job) =>
      log.info(s"[${job.jobId}] RETRY JOB (queue proxy)")
      waitingRegion ! RetryConfirmator(queueName, job)
  }

  private def sendJob(job: ProxyJob, consumers: List[String]) = {
    log.info(s"[${job.jobId}] RECEIVED (queue proxy)")
    waitingRegion ! StartWaitingConfirmator(consumers, job, queueName, queueRegion)
  }

  private def updateNextWorkerToVisit() {
    nextWorkerToVisit += 1
    if (nextWorkerToVisit >= consumerNames.size)
      nextWorkerToVisit = 0
  }

}

package com.omnipresent.model

import akka.actor.{ Actor, ActorLogging, Props, _ }
import akka.cluster.sharding.ClusterSharding
import akka.routing._
import com.omnipresent.model.Consumer.Job
import com.omnipresent.model.MessagesQueue.{ HeartBeat, ProxyJob, RetryJob }
import com.omnipresent.support.IdGenerator

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

  var workersNames: List[String] = List.empty
  var nextWorkerToVisit: Int = 0
  val spread: RoutingLogic = if (spreadType.equalsIgnoreCase("PubSub")) BroadcastRoutingLogic() else RoundRobinRoutingLogic()
  val queueShardName: String = if (spreadType.equalsIgnoreCase("PubSub")) MessagesQueue.pubSubShardName else MessagesQueue.broadcastShardName
  val queueRegion: ActorRef = ClusterSharding(context.system).shardRegion(queueShardName)
  private val consumerRegion: ActorRef = ClusterSharding(context.system).shardRegion(Consumer.shardName)

  override def preStart(): Unit = {
    workersNames = 1 to workers map {
      idx =>
        s"${queueName}-WORKER-${idx}"
    } toList

    log.info(s"Messages queue proxy started with [${workersNames.size}] workers")
  }

  override def receive: Receive = {

    /** Main dispatcher job accordingly to spread type **/
    case job: ProxyJob =>
      spread match { // TODO: Improvement with Composition :)
        case _: BroadcastRoutingLogic =>
          sendJob(job, workersNames)
        case _: RoundRobinRoutingLogic =>
          sendJob(job, List(workersNames(nextWorkerToVisit)))
          updateNextWorkerToVisit()
      }

    /** To retry job with some specific workers **/
    case RetryJob(job, retryWorkers) =>
      sendJob(job, retryWorkers)
  }

  private def sendJob(job: ProxyJob, workers: List[String]) = {
    log.info(s"[${job.jobId}] RECEIVED (queue proxy)")
    val confirmatorProps = Props(new WaitingConfirmator(workers, job, queueName, queueRegion));
    val waitingConfirmator = context.actorOf(confirmatorProps)
    workers map {
      name =>
        consumerRegion ! Job(
          consumerName = IdGenerator.getRandomID(name),
          jobId = job.jobId,
          replyTo = waitingConfirmator)
    }
  }

  private def updateNextWorkerToVisit() {
    nextWorkerToVisit += 1
    if (nextWorkerToVisit >= workersNames.size)
      nextWorkerToVisit = 0
  }

}

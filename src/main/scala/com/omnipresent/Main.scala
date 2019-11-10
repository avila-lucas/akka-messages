package com.omnipresent

import akka.actor.{ ActorRef, ActorSystem }
import akka.cluster.ddata.{ DistributedData, SelfUniqueAddress }
import akka.cluster.sharding.{ ClusterSharding, ClusterShardingSettings }
import akka.cluster.singleton.{ ClusterSingletonProxy, ClusterSingletonProxySettings }
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.omnipresent.model.{ Consumer, MessagesQueue }
import com.omnipresent.support.ClusterListener
import com.omnipresent.support.ClusterListener.GetRoutes
import com.omnipresent.system.Master.CreateQueue
import com.omnipresent.system.MasterSingleton
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration.{ Duration, _ }
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.util.{ Failure, Success }

object AkkaMessages {

  def main(args: Array[String]): Unit = {
    if (args.isEmpty)
      startup(Seq("2551", "2552", "0"))
    else
      startup(args)
  }

  def startup(ports: Seq[String]): Unit = {
    ports foreach { port =>
      val config = ConfigFactory.parseString("akka.remote.artery.canonical.port=" + port).
        withFallback(ConfigFactory.load())

      val system = ActorSystem("akkaMessages", config)

      DistributedData(system).replicator

      val consumerRegion = ClusterSharding(system).start(
        typeName = Consumer.shardName,
        entityProps = Consumer.props(),
        settings = ClusterShardingSettings(system),
        extractEntityId = Consumer.entityIdExtractor,
        extractShardId = Consumer.shardIdExtractor)

      ClusterSharding(system).start(
        typeName = MessagesQueue.broadcastShardName,
        entityProps = MessagesQueue.props("broadcast", consumerRegion),
        settings = ClusterShardingSettings(system),
        extractEntityId = MessagesQueue.entityIdExtractor,
        extractShardId = MessagesQueue.shardIdExtractor)

      ClusterSharding(system).start(
        typeName = MessagesQueue.pubSubShardName,
        entityProps = MessagesQueue.props("PubSub", consumerRegion),
        settings = ClusterShardingSettings(system),
        extractEntityId = MessagesQueue.entityIdExtractor,
        extractShardId = MessagesQueue.shardIdExtractor)

      MasterSingleton.startSingleton(system = system)

      val masterProxy: ActorRef = system.actorOf(
        ClusterSingletonProxy.props(
          singletonManagerPath = "/user/master",
          settings = ClusterSingletonProxySettings(system)),
        name = "masterProxy")

      masterProxy ! CreateQueue(s"queue_$port", 1, 1, 1) // Just an example queue created by default

      if (port == "2551")
        new HttpApi(system)
    }
  }

}

class HttpApi(_system: ActorSystem) {

  implicit val system: ActorSystem = _system
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = system.dispatcher
  implicit val timeout: Timeout = Timeout(5.seconds)

  (system.actorOf(ClusterListener.props()) ? GetRoutes).mapTo[Route].onComplete {

    case Success(routes) =>
      val serverBinding: Future[Http.ServerBinding] = Http().bindAndHandle(routes, "localhost", 8080)
      serverBinding.onComplete {
        case Success(bound) =>
          println(s"Server online at http://${bound.localAddress.getHostString}:${bound.localAddress.getPort}/")
        case Failure(e) =>
          Console.err.println(s"Server could not start!")
          e.printStackTrace()
          system.terminate()
      }

    case Failure(_) =>
      print("AAAAA, ERRROR")

  }

  Await.result(system.whenTerminated, Duration.Inf)

}
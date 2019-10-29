package com.omnipresent

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.omnipresent.model.{Consumer, MessagesQueue}
import com.omnipresent.system.MasterSingleton
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

object Main extends App with QueueRoutes {

  implicit val system: ActorSystem = ActorSystem("akkaMessages-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = system.dispatcher

  startup(Seq(2551, 2552))
  val masterProxy = system.actorOf(MasterSingleton.proxyProps(system), name = "masterProxy")

  lazy val routes: Route = queueRoutes

  val serverBinding: Future[Http.ServerBinding] = Http().bindAndHandle(routes, "localhost", 8080)

  serverBinding.onComplete {
    case Success(bound) =>
      println(s"Server online at http://${bound.localAddress.getHostString}:${bound.localAddress.getPort}/")
    case Failure(e) =>
      Console.err.println(s"Server could not start!")
      e.printStackTrace()
      system.terminate()
  }

  Await.result(system.whenTerminated, Duration.Inf)

  def startup(ports: Seq[Int]) =
    ports foreach {
      port =>
        val system = ActorSystem("akkaMessages-system")

        val consumerRegion = ClusterSharding(system).start(
          typeName = Consumer.shardName,
          entityProps = Consumer.props(),
          settings = ClusterShardingSettings(system),
          extractEntityId = Consumer.entityIdExtractor,
          extractShardId = Consumer.shardIdExtractor)

        val broadcastQueuesRegion = ClusterSharding(system).start(
          typeName = MessagesQueue.broadcastShardName,
          entityProps = MessagesQueue.props("broadcast", consumerRegion),
          settings = ClusterShardingSettings(system),
          extractEntityId = MessagesQueue.entityIdExtractor,
          extractShardId = MessagesQueue.shardIdExtractor)

        val pubSubQueuesRegion = ClusterSharding(system).start(
          typeName = MessagesQueue.pubSubShardName,
          entityProps = MessagesQueue.props("PubSub", consumerRegion),
          settings = ClusterShardingSettings(system),
          extractEntityId = MessagesQueue.entityIdExtractor,
          extractShardId = MessagesQueue.shardIdExtractor)

        if(port == 2551)
          MasterSingleton.startSingleton(
            system = ActorSystem("akkaMessages-system", config(port, "master")),
            broadcastRegion = broadcastQueuesRegion,
            pubSubRegion = pubSubQueuesRegion
          )
    }

  def config(port: Int, role: String): Config =
    ConfigFactory.parseString(
      s"""
      akka.remote.netty.tcp.port=$port
      akka.cluster.roles=[$role]
    """).withFallback(ConfigFactory.load())
}


package com.omnipresent

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.omnipresent.system.MasterSingleton
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

object Main extends App with QueueRoutes {

  implicit val system: ActorSystem = ActorSystem("akkaMessages-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = system.dispatcher

  startNode(2551)
  val masterProxy = system.actorOf(MasterSingleton.proxyProps(system), name = "masterProxy")
  startNode(2552)
  startNode(2553)
  startNode(2554)

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

  def config(port: Int, role: String): Config =
    ConfigFactory.parseString(s"""
      akka.remote.netty.tcp.port=$port
      akka.cluster.roles=[$role]
    """).withFallback(ConfigFactory.load())

  def startNode(port: Int): ActorRef = {
    val system = ActorSystem("akkaMessages-system", config(port, "night-watch"))
    MasterSingleton.startSingleton(system)
  }
}


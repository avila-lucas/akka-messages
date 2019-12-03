package com.omnipresent

import akka.actor.{ ActorRef, ActorSystem }
import akka.event.Logging
import akka.http.scaladsl.model.{ ContentTypes, HttpEntity, StatusCodes }
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.MethodDirectives.{ get, post }
import akka.http.scaladsl.server.directives.PathDirectives.path
import akka.http.scaladsl.server.directives.RouteDirectives.complete
import akka.pattern.ask
import akka.util.Timeout
import com.omnipresent.support.{ JsonSupport, ViewSupport }
import com.omnipresent.system.Master._
import com.omnipresent.system.{ ProducerCreationResult, QueuesNames }

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

trait QueueRoutes extends JsonSupport {

  implicit def system: ActorSystem

  lazy val log = Logging(system, classOf[QueueRoutes])

  def masterProxy: ActorRef

  implicit lazy val timeout: Timeout = Timeout(5.seconds)

  lazy val queueRoutes: Route =
    pathPrefix("queues") {
      concat(
        pathEnd {
          concat(
            get {
              val queuesJson = Await.result((masterProxy ? GetQueuesNames).mapTo[QueuesNames], Duration.Inf)
              val data = ViewSupport.parseData(queuesJson.queues)

              val path = System.getProperty("user.dir") + "/src/main/resources/index.html"
              val source = scala.io.Source.fromFile(path)
              val lines = try source.mkString finally source.close()

              complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, lines.replace("<QUEUE>", data)))
            },
            post {
              entity(as[CreateQueue]) { queue =>
                val queueCreated: Future[ActionPerformed] =
                  (masterProxy ? queue).mapTo[ActionPerformed]
                onSuccess(queueCreated) { performed =>
                  log.info(s"Created queue [${queue.name}] with ${queue.producers} producers and ${queue.workers} workers")
                  complete((StatusCodes.Created, performed))
                }
              }
            })
        },
        path(Segment) { name =>
          concat(
            //            get {
            //              val maybeQueueInfo =
            //                (master ? GetQueue(name)).mapTo[QueueInfo]
            //              complete(maybeQueueInfo)
            //            },
            post {
              entity(as[CreateProducer]) { newProducer =>
                val producerCreated: Future[ProducerCreationResult] =
                  (masterProxy ? newProducer).mapTo[ProducerCreationResult]
                onSuccess(producerCreated) { performed =>
                  log.info(s"Created producer [${performed.created}] for [${newProducer.queueName}] with capacity ${newProducer.transactional}")
                  complete((StatusCodes.Created, performed))
                }
              }
            })
        })
    }
}

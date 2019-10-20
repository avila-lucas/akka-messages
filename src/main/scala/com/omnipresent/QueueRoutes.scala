package com.omnipresent

import akka.actor.{ ActorRef, ActorSystem }
import akka.event.Logging
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.MethodDirectives.{ get, post }
import akka.http.scaladsl.server.directives.PathDirectives.path
import akka.http.scaladsl.server.directives.RouteDirectives.complete
import akka.pattern.ask
import akka.util.Timeout
import com.omnipresent.AkkaMessagesSupervisor._
import com.omnipresent.support.JsonSupport

import scala.concurrent.Future
import scala.concurrent.duration._

trait QueueRoutes extends JsonSupport {

  implicit def system: ActorSystem

  lazy val log = Logging(system, classOf[QueueRoutes])

  def akkaMessagesSupervisor: ActorRef

  implicit lazy val timeout: Timeout = Timeout(5.seconds)

  lazy val queueRoutes: Route =
    pathPrefix("queues") {
      concat(
        pathEnd {
          concat(
            get {
              val queues = (akkaMessagesSupervisor ? GetQueuesNames).mapTo[QueuesNames]
              complete(queues)
            },
            post {
              entity(as[CreateQueue]) { queue =>
                val queueCreated: Future[ActionPerformed] =
                  (akkaMessagesSupervisor ? queue).mapTo[ActionPerformed]
                onSuccess(queueCreated) { performed =>
                  log.info(s"Created queue [${queue.name}] with ${queue.producers} producers and ${queue.workers} workers")
                  complete((StatusCodes.Created, performed))
                }
              }
            })
        },
        path(Segment) { name =>
          concat(
            get {
              val maybeQueueInfo =
                (akkaMessagesSupervisor ? GetQueue(name)).mapTo[QueueInfo]
              complete(maybeQueueInfo)
            },
            post {
              entity(as[CreateProducer]) { newProducer =>
                val producerCreated: Future[ProducerCreationResult] =
                  (akkaMessagesSupervisor ? newProducer).mapTo[ProducerCreationResult]
                onSuccess(producerCreated) { performed =>
                  log.info(s"Created producer [${performed.created}] for [${newProducer.queueName}] with capacity ${newProducer.transactional}")
                  complete((StatusCodes.Created, performed))
                }
              }
            })
        })
    }
}

package com.omnipresent.support

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.omnipresent.AkkaMessagesSupervisor.{ ActionPerformed, CreateProducer, CreateQueue }
import com.omnipresent.{ ProducerCreationResult, QueueInfo, QueuesNames }
import spray.json.DefaultJsonProtocol

trait JsonSupport extends SprayJsonSupport {
  // import the default encoders for primitive types (Int, String, Lists etc)

  import DefaultJsonProtocol._

  implicit val queueNamesFormat = jsonFormat1(QueuesNames)
  implicit val queueInfoFormat = jsonFormat1(QueueInfo)
  implicit val producerCreationResultFormat = jsonFormat1(ProducerCreationResult)
  implicit val createProducerFormat = jsonFormat2(CreateProducer)
  implicit val createQueueFormat = jsonFormat5(CreateQueue)

  implicit val actionPerformedJsonFormat = jsonFormat1(ActionPerformed)
}

package com.omnipresent.model

import com.omnipresent.model.MessagesQueue.Start
import com.omnipresent.model.Producer.DeliverJob

sealed trait Event

case class MsgStartQueue(start: Start) extends Event

case class MsgToSend(job: DeliverJob) extends Event

case class MsgConfirmed(jobId: String, deliveryId: Long) extends Event

case class MsgFailed(jobId: String, deliveryId: Long) extends Event
package com.omnipresent.support

import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{ MemberDowned, MemberUp }
import akka.cluster.singleton.{ ClusterSingletonProxy, ClusterSingletonProxySettings }
import com.omnipresent.QueueRoutes
import com.omnipresent.support.ClusterListener.GetRoutes

object ClusterListener {

  case object GetRoutes

  def props() = Props[ClusterListener]
}

class ClusterListener
  extends Actor
  with QueueRoutes {

  val system: ActorSystem = context.system
  val cluster: Cluster = Cluster(context.system)
  val masterProxy: ActorRef = context.actorOf(
    ClusterSingletonProxy.props(
      singletonManagerPath = "/user/master",
      settings = ClusterSingletonProxySettings(context.system)),
    name = "masterProxy")

  override def preStart(): Unit = cluster.subscribe(self, classOf[MemberDowned], classOf[MemberUp])

  override def postStop(): Unit = cluster.unsubscribe(self)

  override def receive: Receive = {
    case GetRoutes =>
      sender() ! queueRoutes
    case MemberDowned(_) =>
      log.info("MEMBER DOWNED!")
      log.info(s"Cluster state: ${cluster.state}")
    case MemberUp(_) =>
      log.info("MEMBER UP!")
      log.info(s"Cluster state: ${cluster.state}")
  }

}

package com.omnipresent.system

import akka.actor.{ ActorRef, ActorSystem, PoisonPill, Props }
import akka.cluster.singleton.{ ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings }

object MasterSingleton {

  private val singletonName = "master"

  def startSingleton(system: ActorSystem): ActorRef = {
    system.actorOf(
      ClusterSingletonManager.props(
        Master.props,
        PoisonPill,
        ClusterSingletonManagerSettings(system)),
      singletonName)
  }

  def proxyProps(system: ActorSystem): Props = ClusterSingletonProxy.props(
    settings = ClusterSingletonProxySettings(system).withSingletonName(singletonName),
    singletonManagerPath = s"/user/$singletonName")
}

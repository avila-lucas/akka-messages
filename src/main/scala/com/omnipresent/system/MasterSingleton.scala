package com.omnipresent.system

import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}

object MasterSingleton {

  private val singletonName = "master"
  private val singletonRole = "night-watch"

  def startSingleton(system: ActorSystem): ActorRef = {
    system.actorOf(
      ClusterSingletonManager.props(
        Master.props(),
        PoisonPill,
        ClusterSingletonManagerSettings(system).withRole(singletonRole)
      ),
      singletonName)
  }

  def proxyProps(system: ActorSystem): Props = ClusterSingletonProxy.props(
    settings = ClusterSingletonProxySettings(system).withRole(singletonRole),
    singletonManagerPath = s"/user/$singletonName")
}

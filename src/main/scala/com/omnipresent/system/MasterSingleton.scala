package com.omnipresent.system

import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}

object MasterSingleton {

  private val singletonName = "master"
  private val singletonRole = "master"

  def startSingleton(system: ActorSystem, broadcastRegion: ActorRef, pubSubRegion: ActorRef): ActorRef = {
    system.actorOf(
      ClusterSingletonManager.props(
        Master.props(broadcastRegion, pubSubRegion),
        PoisonPill,
        ClusterSingletonManagerSettings(system).withRole(singletonRole)
      ),
      singletonName)
  }

  def proxyProps(system: ActorSystem): Props = ClusterSingletonProxy.props(
    settings = ClusterSingletonProxySettings(system).withRole(singletonRole),
    singletonManagerPath = s"/user/$singletonName")
}

/*
 *
 *
 *  Copyright 2015 eBay, Inc.
 *
 *  AIALib: Asynchronous Iterative Algorithm Library
 *  Author: Alexander Terenin
 *
 *  Licensed under the Apache License, Version 2.0 (the "License")
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package aialib

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster._
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator._
import akka.remote._

class Control[V](P: Class[Problem[V]]) extends Actor {
  val numberOfMachines = P.newInstance.numberOfMachines //use with lazy evaluation to avoid loading data

  val mediator = DistributedPubSub(context.system).mediator
  mediator ! Subscribe("control", self)

  val cluster = Cluster(context.system)

  override def preStart() {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent])
  }

  override def postStop() {
    cluster.unsubscribe(self)
    cluster.leave(cluster.selfAddress)
    context.system.terminate()
  }

  def receive = receiveF(Set.empty)

  def receiveF(clusterMembers: Set[Member]): Actor.Receive = {
    case MemberUp(member) if member.getRoles.contains("worker") =>
      val newClusterMembers = clusterMembers + member
      println(s"worker up: ${member.address}")
      context.become(receiveF(newClusterMembers))

      if (newClusterMembers.size == numberOfMachines) {
        self ! Start
      }

    case MemberRemoved(member, previousStatus) if member.getRoles.contains("worker") =>
      val newClusterMembers = clusterMembers - member
      println(s"worker down: ${member.address}")
      context.become(receiveF(newClusterMembers))
      mediator ! Publish("control", Finish)

      if (newClusterMembers.isEmpty) {
        println("shutting down")
        self ! PoisonPill
      }

    case Start =>
      for ((member, port) <- clusterMembers.map(member => (member, member.address.hostPort.replaceAll(".*:","").toInt))) {
        val address = member.address
        println(s"creating worker id: $port, address: $address")

        val child = context.actorOf(
          Props(classOf[Worker[V]], port, P)
            .withDispatcher("custom-dispatcher")
            .withDeploy(Deploy(scope = RemoteScope(address))),
          name = port.toString
        )
      }

    case Finish =>
      Thread.sleep(1000)

      if (clusterMembers.size != 0) {
        mediator ! Publish("control", Finish)
      }
  }
}

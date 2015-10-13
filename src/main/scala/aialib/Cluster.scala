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

import akka.actor.{Props, ActorSystem}
import scala.reflect.{ClassTag, classTag}
import com.typesafe.config.ConfigFactory

class Cluster[problemType <: Problem[variableIdType] : ClassTag, variableIdType](hostname: String, port: String, controlURL: String) {
  //Akka doesn't like ClassTags, so pass around val P = Class[Problem] instead
  val problemClass = classTag[problemType].runtimeClass.asInstanceOf[Class[problemType]]

  val controlPort = controlURL.replaceAll("([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+:)","")
  val clusterRole = if(controlPort == port) "control" else "worker"

  val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port = $port")
    .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.hostname = $hostname"))
    .withFallback(ConfigFactory.parseString("akka.cluster.seed-nodes = [\"akka.tcp://actorsystem@" + controlURL + "\"]")) //scala bug
    .withFallback(ConfigFactory.parseString(s"akka.cluster.roles = [$clusterRole]"))
    .withFallback(ConfigFactory.load())

  val system = ActorSystem("actorsystem", config)

  if(clusterRole == "control") {
    val control = system.actorOf(
      Props(classOf[aialib.Control[variableIdType]], problemClass)
        .withDispatcher("custom-dispatcher"),
      name = "control"
    )
  }
}

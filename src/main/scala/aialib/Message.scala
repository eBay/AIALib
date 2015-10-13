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

//import java.util.concurrent.ConcurrentHashMap
//import scala.annotation.tailrec
//import scala.collection.JavaConversions._
//import akka.actor.{ActorRef, ActorSystem}
//import akka.dispatch._
//import com.typesafe.config.Config

sealed trait Message
case object Start extends Message
case class Update[V](workerId: Int, variableId: V, value: Any) extends Message
case object Finish extends Message

//class PriorityQueueMailbox(settings: ActorSystem.Settings, cfg: Config) extends UnboundedStablePriorityMailbox(
//  PriorityGenerator {
//    case Update => 3
//    case Finish => 2
//    case _ => 1
//  }
//)

//class CustomMailbox(settings: ActorSystem.Settings, cfg: Config)
//  extends MailboxType with ProducesMessageQueue[CustomQueue] {
//  final override def create(owner: Option[ActorRef], system: Option[ActorSystem]): MessageQueue =
//    new CustomQueue()
//}
//
//class CustomQueue()
//  extends AbstractNodeQueue[Envelope] with MessageQueue with UnboundedMessageQueueSemantics {
//  val latestOnlyVariables = new ConcurrentHashMap[(Int, Any), Envelope]
//
//  def enqueue(receiver: ActorRef, handle: Envelope): Unit = {
//    handle.message match {
//      case Update(workerId, variableId, value) => {
//        latestOnlyVariables.put((workerId, variableId), handle)
//      }
//      case _ => add(handle) //super.enqueue(receiver, handle)
//    }
//  }
//
//  def dequeue(): Envelope = {
//    Option(poll()) //super.dequeue(receiver, handle)
//      .orElse(
//        latestOnlyVariables.headOption
//          .map({ k => latestOnlyVariables.remove(k._1); k._2 })
//      )
//      .orNull
//  }
//  def numberOfMessages: Int = {
//    count() + latestOnlyVariables.size
//  }
//  def hasMessages: Boolean = {
//    !isEmpty() || !latestOnlyVariables.isEmpty
//  }
//
//  @tailrec final def cleanUp(owner: ActorRef, deadLetters: MessageQueue): Unit = {
//    val envelope = dequeue()
//    if (envelope ne null) {
//      deadLetters.enqueue(owner, envelope)
//      cleanUp(owner, deadLetters)
//    }
//  }
//}
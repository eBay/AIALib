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

import java.io._
import java.util.concurrent.{ConcurrentLinkedQueue, ConcurrentHashMap}
import java.util.concurrent.atomic.{AtomicLong, AtomicBoolean}
import scala.collection.JavaConversions._
import akka.actor._
import akka.cluster._
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator._
import breeze.linalg.{ Vector => _, _ }

class Worker[V](workerId: Int, P: Class[Problem[V]]) extends Actor {
  val problem = P.newInstance
  println(s"loading problem $P")

  val workloadMap = problem.workloadMap

  val nMC = problem.nMC
  val localWorkload = problem.workloadMap(workerId)
  val localVariables = localWorkload.reduce(_ union _)
  val storedVariables = problem.storedVariables

  val latest = new ConcurrentHashMap[V, Any](mapAsJavaMap(problem.getInitial))
  val nUpdates = new AtomicLong(0L)
  val flowing = new AtomicBoolean(true)
  private val runnables = new ConcurrentLinkedQueue[updRunnable]

  val out = localVariables.intersect(storedVariables).map(_ -> new ConcurrentLinkedQueue[Any]()).toMap

  val mediator = DistributedPubSub(context.system).mediator
  mediator ! Subscribe("updates", self)
  mediator ! Subscribe("control", self)

  problem.setRandomNumberSeed(workerId)

  println(s"worker up: $workerId")

  startRunnables().foreach(runnables.add(_))




  private def startRunnables() = {
    localWorkload
      .zipWithIndex
      .map{
        case (workload, threadId) => {
          val r = new updRunnable(threadId)
          new Thread(r).start()
          r
        }
      }
      .toSet
  }

  private def stopRunnables(): Unit = {
    runnables.foreach(_.stop())
    Thread.sleep(1000)
  }

  private class updRunnable(threadId: Int) extends Runnable {
    private val running = new AtomicBoolean(false)    //must be atomic due to race conditions associated with
    private val nSamples = new AtomicLong(0L)         //stopping (and to avoid mixing vars with concurrency)
    def run(): Unit = {
      running.set(true)
      println(s"thread up: $threadId")
      while (running.get == true) {
        val (nextId, nextValue) = problem.update(workerId, threadId, latest)

        val s = nSamples.incrementAndGet()

        val oldValue = latest.replace(nextId, nextValue)
        problem.processUpdate(nextId, nextValue, oldValue, latest)

        if (storedVariables.contains(nextId)) {
          out(nextId).add(nextValue)
        }

        if(s % math.ceil(0.05 * nMC.toDouble).toInt == 0 || s == math.ceil(0.001 * nMC.toDouble).toInt) {
          println(s"total samples for worker $workerId thread $threadId: $s/$nMC")
        }

        if(s >= nMC) {
          mediator ! Publish("control", Finish)
          context.become(finishing)
          this.stop()
          stopRunnables()
        }

        if( (problem.thinning(nextId) == true //order dependent: don't change flowing unless thinning == true
          && flowing.compareAndSet(true, false) == true //try to change from true to false, return true if successful
          ) || s % math.ceil(0.01 * nMC.toDouble).toInt == 0 ) {
            mediator ! Publish("updates", Update(workerId, nextId, nextValue))
        }

        java.util.concurrent.locks.LockSupport.parkNanos(100000L) //give Akka CPU time to clear mailbox, edge cases
      }
    }
    def stop(): Unit = {
      if(running.getAndSet(false) == true) {
        println(s"thread stopped: $threadId, samples drawn: ${nSamples.get}")
      }
    }
  }

  override def postStop() {
    println(s"worker down: $workerId")
    stopRunnables() //only called if actor shutdown remotely, for example because it became unreachable
    val cluster = Cluster(context.system)
    cluster.leave(cluster.selfAddress) //will trigger machine down on control
    Thread.sleep(5000)
    context.system.terminate()
  }

  def receive = working orElse finishing

  def working: Actor.Receive = {
    /*
    * below: the functions being called downstream will throw errors if types are incorrect,
    * and we cannot check here anyway because the types are eliminated by erasure,
    * so just suppress the compiler warnings by adding @unchecked
    * */
    case Update(fromWorkerId, variableId: V @unchecked, value) if fromWorkerId != workerId && !localVariables.contains(variableId) =>
      val oldValue = latest.replace(variableId, value)
      problem.processUpdate(variableId, value, oldValue, latest)
      nUpdates.incrementAndGet()

    case Update(fromWorkerId, variableId: V @unchecked, value) if fromWorkerId == workerId =>
      flowing.set(true)
  }

  def finishing: Actor.Receive = {
    case Finish =>
      stopRunnables()
      println(s"worker stopped: $workerId, updates received: ${nUpdates.get()}")

      Worker.writeOutput(workerId.toString, out)

      self ! PoisonPill
      context.become(done)
  }

  def done: Actor.Receive = PartialFunction.empty
}

object Worker {
  def writeOutput(idString: String, out: Map[_, ConcurrentLinkedQueue[Any]]) = {
    out.foreach {
      case (outVar, outQueue) => {
        val fileName = "output/out-"
          .concat(idString)
          .concat("-")
          .concat(outVar.toString.take(64).replaceAll("[( ,)]",""))
        println(s"writing output from worker $idString of length ${outQueue.size} to $fileName for variable $outVar")

        val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName)))
        outQueue.toArray.foreach{
          case vec: DenseVector[_] => writer.write(s"${vec.data.mkString(",")}\n")
          case mat: DenseMatrix[_] => writer.write(s"${mat.data.mkString(",")}\n")
          case line => writer.write(s"$line\n")
        }
        writer.close()
      }
    }
  }
}
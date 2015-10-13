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

package aialib.examples

import java.util.concurrent.ConcurrentHashMap
import aialib.{Cluster, Problem}
import breeze.linalg._
import breeze.stats.distributions._

object ToyExampleProblem extends App {
  val hostname = args(0)
  val port = args(1)
  val controlURL = args(2)
  new Cluster[ToyExampleProblem, Int](hostname, port, controlURL)
}

class ToyExampleProblem extends Problem[Int] with Sampling {
  def startValues = DenseVector(10.01, 10.02, 10.03, 10.04, 10.05, 10.06, 10.07, 10.08)
  val dim = startValues.length

  def nMC: Long = 100000

//  val workloadMap = Map(2552 -> IndexedSeq((0 until 8).toSet))

//  def nMachines = if(dim % 2 == 0) 2 else 1
//  def nThreadsPerMachine = if(dim % 2 == 0) dim/2 else dim

  def nMachines = 8
  def nThreadsPerMachine = 1

  val workloadMap = (1 to nMachines)
    .map(v => v+2551)
    .map(port => (port, 0 until nThreadsPerMachine))
    .map{case (port, rng) => {
      val vstart = (port - 2552) * nThreadsPerMachine
      val vrng = rng.map(_ + vstart)
        .map(Set(_))
      (port, vrng)
    }
    }
    .toMap

  def thinning(variable: Int) = {
//    if(Uniform(0,1).draw() < 0.1) {
      true
//    } else {
//      false
//    }
  }

  def getInitial = {
    val out = startValues.toArray.zipWithIndex.map{
      case (v, idx) => (idx, v)
    }.toMap
    out
  }

  def update(workerId: Int, threadId: Int, latest: ConcurrentHashMap[Int, Any]) = {
    val latestArray = latest.values.toArray.map(_.asInstanceOf[Double])
    val latestVector = new DenseVector(latestArray)
    val variableIds = workloadMap
      .apply(workerId)
      .apply(threadId)
      .toArray
    val variableId = variableIds(Rand.generator.nextInt(variableIds.length))
    val nextVar = updateNormal(variableId, latestVector, mu, Sigma)
    (variableId, nextVar)
  }

  def setRandomNumberSeed(workerId: Int) = Rand.generator.setSeed(workerId * 1000000) //really bad solution: change later

  def storedVariables: Set[Int] = (0 until dim).toSet





  val mu = DenseVector.zeros[Double](dim)
  val Sigma = DenseMatrix.tabulate(dim, dim)({
    case (i,j) =>
      val tau = 1
      val phi = .5
      val alpha = 1
      math.pow(tau, 2) * math.exp(-phi * math.pow(math.abs(i - j), alpha))
  })

  def updateNormal(idx: Int, oldx: DenseVector[Double],
                   mu: DenseVector[Double], Sigma: DenseMatrix[Double]): Double = {
    val sidx = Seq(idx) //need to cast idx to seq to avoid type confusion
    val midx = (0 until oldx.length).filter({ value => value != idx })

    val x2 = oldx(midx)

    val mu1 = mu(sidx)
    val mu2 = mu(midx)

    val Sigma11 = Sigma(sidx,sidx)
    val Sigma22 = Sigma(midx, midx)
    val Sigma12 = Sigma(sidx, midx)
    val Sigma21 = Sigma(midx, sidx)

    val Sigma22inv = breeze.linalg.inv(Sigma22.toDenseMatrix)

    val mustar = (mu1 + (Sigma12 * Sigma22inv * (x2 - mu2))).toArray.head
    val Sigmastar = (Sigma11 - (Sigma12 * Sigma22inv * Sigma21)).toDenseMatrix.toArray.head

    val stddev = breeze.numerics.sqrt(Sigmastar)

    val out = Gaussian(mustar, stddev).draw()
    out
  }
}

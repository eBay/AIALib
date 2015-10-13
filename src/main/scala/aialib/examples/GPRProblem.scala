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
import breeze.linalg.CSCMatrix._
import breeze.linalg.DenseMatrix._
import breeze.linalg.DenseVector._
import breeze.linalg._
import breeze.stats.distributions._
import org.apache.commons.math3.random.MersenneTwister

/*
* warning:
*
* variables used inside here may not conform to scala naming conventions
* because they are named according to mathematical conventions which
* distinguish between upper and lower-case greek letters
*
* please do not alter this naming style
*/

object GPRProblem extends App {
  val hostname = args(0)
  val port = args(1)
  val controlURL = args(2)
  new Cluster[GPRProblem,GPRVariables.anyVar](hostname, port, controlURL)
}

object GPRVariables {
  sealed trait anyVar
  case class thetaslice(id: Range) extends anyVar
  case object mu extends anyVar
  case object sigmasq extends anyVar
  case object tausq extends anyVar
}

class GPRProblem extends Problem[GPRVariables.anyVar] with Sampling {
  def nMC: Long = 40000
  def nMachines = 12
  def nThreadsPerMachine = 8

  val nWorkers = nMachines * nThreadsPerMachine
  val workloadMap = (0 until nMachines)
    .map(
      workerId => {
        val port = workerId+2552
        val varsPerThread = (0 until nThreadsPerMachine)
          .map(threadId => ourVariableIds(workerId, threadId))
        (port, varsPerThread)
      }
    )
    .toMap
  def ourVariableIds(workerId: Int, threadId: Int): Set[GPRVariables.anyVar] = {
    val uniqueId = workerId * nThreadsPerMachine + threadId
//    val thetas = GPRVariables.thetaslice(uniqueId*(dims.n/nWorkers) until (uniqueId+1)*(dims.n/nWorkers))
    val thetaMin = uniqueId*(dims.n/nWorkers)
    val thetaMax = (uniqueId+1)*(dims.n/nWorkers)
    val thetaMid = (thetaMin + thetaMax) / 2
    val thetas1 = GPRVariables.thetaslice(thetaMin until thetaMid)
    val thetas2 = GPRVariables.thetaslice(thetaMid until thetaMax)
    Set.empty[GPRVariables.anyVar] + thetas1 + thetas2 + GPRVariables.mu + GPRVariables.sigmasq + GPRVariables.tausq
  }




  object data {
    val rawData = generateData()
    val x = rawData._1
    val y = rawData._2
  }
  object dims {
    val min = -3168.0 //-1440.0 //-3218.0 //-2145.0
    val max = 3168.0 //1440.0 //3217.0 //2145.0
    val gridsize = 0.06 //do not change: fixing phi to 0.5 depends on this grid size!
    val n = (min until max by gridsize).length
  }

  object fixedMatrices {
//    val Hphi = DenseMatrix.tabulate[Double](dims.n,dims.n){
//      case (i,j) => math.exp(-tuningParameters.phi * math.abs(data.x(i) - data.x(j)))
//    }
    val Hinvphi = CSCMatrix.tabulate[Double](dims.n,dims.n){
      case (i,j) => {
        val n = dims.n
        val phi = tuningParameters.phi
        val gridsize = dims.gridsize
        val out = if((i==0 && j==0) || (i==n-1 && j==n-1)) {
          (
            (
             (
             math.exp(-phi * gridsize * ((2 * n - 3).toDouble))
             ) / math.sinh(-phi * gridsize)
            ) + (1.0 - 1.0/math.tanh(-phi * gridsize))
          ) / (
            2.0 - 2.0 * math.exp(-phi * gridsize * ((2 * n - 3).toDouble))
          )
        } else if(i==j) { //d
          -1.0 / math.tanh(-phi * gridsize)
        } else if(math.abs(i-j) == 1) {
          0.5 / math.sinh(-phi * gridsize)
        } else {
          0.0
        }
        out
      }
    }
    //val Hinvphi1n = Hinvphi * DenseVector.ones[Double](dims.n)
    val Hinvphi1nEdge = (Hinvphi(0 until 3, 0 until 3) * DenseVector.ones[Double](3)).apply(0)
    val Hinvphi1nMiddle = (Hinvphi(0 until 3, 0 until 3) * DenseVector.ones[Double](3)).apply(1)
    val t1nHinvphi1n = 2.0 * Hinvphi1nEdge + (dims.n-2).toDouble * Hinvphi1nMiddle
  }
  object tuningParameters {
    val phi = 0.5
    val aMu = sum(data.y) / data.y.length.toDouble
    val bMu = (max(data.y) - min(data.y))/6.0 * (max(data.y) - min(data.y))/6.0
    val aSigma = 2.0
    val bSigma = bMu
    val aTau = aSigma
    val bTau = bMu
  }






  def setRandomNumberSeed(workerId: Int) = Rand.generator.setSeed(workerId * 1000000) //really bad solution: change later

  def thinning(variable: GPRVariables.anyVar) = variable match {
    case variable:GPRVariables.thetaslice => {
//      if(Uniform(0,1).draw() < 0.2) {
        true
//      } else {
//        false
//      }
    }
    case _ => {
      false
    }
  }

  def getInitial() = {
    val mu = 10.0
    val sigmasq = 10.0
    val tausq = 10.0
    val theta = 0.0

    val out = allThetas
      .map{
        v => (v: @unchecked) match {
          case s@GPRVariables.thetaslice(thetaRng) => (s, DenseVector.zeros[Double](thetaRng.length) :+ theta)
        }
      }.toMap ++ Map[GPRVariables.anyVar, Any](
      GPRVariables.mu -> mu,
      GPRVariables.sigmasq -> sigmasq,
      GPRVariables.tausq -> tausq
    )

    out
  }

  def storedVariables: Set[GPRVariables.anyVar] = {
    val thetas = (0 until nWorkers).flatMap{
      uniqueId => {
        val thetaMin = uniqueId*(dims.n/nWorkers)
        val thetaMax = (uniqueId+1)*(dims.n/nWorkers)
        val thetaMid = (thetaMin + thetaMax) / 2
        val thetas1 = GPRVariables.thetaslice(thetaMin until thetaMid)
        val thetas2 = GPRVariables.thetaslice(thetaMid until thetaMax)
        Set(GPRVariables.thetaslice(thetaMin until thetaMid), GPRVariables.thetaslice(thetaMax until thetaMax))
      }
    }
    Set.empty[GPRVariables.anyVar] + GPRVariables.mu + GPRVariables.sigmasq + GPRVariables.tausq ++ thetas
  }

  val workloadMapThetas = workloadMap.map{
    case (key, arrayOfSets) => {
      val arrayOfVecs = arrayOfSets.map{
        set => set
          .filter{
            case v: GPRVariables.thetaslice => true
            case _ => false
          }
          .toVector
      }
      (key, arrayOfVecs)
    }
  }
  def selectNextVariable(workerId: Int, threadId: Int) = {
    val possibleThetas = workloadMapThetas
      .apply(workerId)
      .apply(threadId)

    val percentageThetas = possibleThetas.size.toDouble / (possibleThetas.size.toDouble + 3.0) //3.0 for mu, sigmasq, tausq
    val percentageNonThetas = 1.0 - percentageThetas
    val probabilityNonThetas = 2.0 * percentageNonThetas / nThreadsPerMachine.toDouble
    val probabilityThetas = 1.0 - probabilityNonThetas

    val out = if(Uniform(0,1).draw() < probabilityThetas) {
      val randomIdx = scala.util.Random.nextInt(possibleThetas.size)
      possibleThetas(randomIdx)
    } else {
      val randomIdx = scala.util.Random.nextInt(3)
      if(randomIdx == 0) GPRVariables.sigmasq
      else if(randomIdx == 1) GPRVariables.tausq
      else GPRVariables.mu
    }
    out
  }

  def update(workerId: Int, threadId: Int,
             latest: ConcurrentHashMap[GPRVariables.anyVar, Any]): (GPRVariables.anyVar, Any) =
    selectNextVariable(workerId, threadId) match {
      case v@GPRVariables.thetaslice(thetaRng) => {
        val newThetaSlice = updateThetaSlice(thetaRng, latest)
        (v, newThetaSlice)
      }
      case v@GPRVariables.mu => {
        val newMu = updateMu(latest)
        (v, newMu)
      }
      case v@GPRVariables.tausq => {
        val newTausq = updateTausq(latest)
        (v, newTausq)
      }
      case v@GPRVariables.sigmasq => {
        val newSigmasq = updateSigmasq(latest)
        (v, newSigmasq)
      }
    }




  val allThetas = workloadMap.values
    .reduce(_ ++ _)
    .reduce(_ union _)
    .filter{
      case v: GPRVariables.thetaslice => true
      case _ => false
    }
    .toArray
    .sortBy{
      v => (v: @unchecked) match {
        case s@GPRVariables.thetaslice(thetaRng) => thetaRng.min
      }
    }
  def getTheta(latest: ConcurrentHashMap[GPRVariables.anyVar, Any]) = {
    val currentThetas = allThetas.flatMap{
      s => {
        latest.get(s).asInstanceOf[DenseVector[Double]].toArray
      }
    }

    new DenseVector(currentThetas)
  }

  def updateThetaSlice(thetaRng: Range, latest: ConcurrentHashMap[GPRVariables.anyVar, Any]) = {
    val n = dims.n
    val sigmasq = latest.get(GPRVariables.sigmasq).asInstanceOf[Double]
    val mu = latest.get(GPRVariables.mu).asInstanceOf[Double]
    val tausq = latest.get(GPRVariables.tausq).asInstanceOf[Double]
    val theta = getTheta(latest)
    val phi = tuningParameters.phi
    val gridsize = dims.gridsize

    val thetaDepRng = max(thetaRng.min - 1, 0) until min(thetaRng.max + 2, n) //all needed values of theta

//    val Hinvphi1n = fixedMatrices.Hinvphi1n
    val Hinvphi1nEdge = fixedMatrices.Hinvphi1nEdge
    val Hinvphi1nMiddle = fixedMatrices.Hinvphi1nMiddle

    val b = (-1.0 / math.tanh(-phi * gridsize)) / tausq + (1.0 / sigmasq) //main diagonal
    val c = (0.5 / math.sinh(-phi * gridsize)) / tausq //off diagonal
    val D = b / c

    val R = DenseMatrix.tabulate[Double](thetaDepRng.size,thetaDepRng.size){ //approximate inverse
        case (mi,mj) => {
          val ri = thetaDepRng(mi)
          val rj = thetaDepRng(mj)
          val i = math.round( math.ceil( (n.toDouble) / 2.0 ) - math.floor( math.abs(ri - rj) / 2.0 ) )
          val j = math.round( math.ceil( (n.toDouble) / 2.0 ) + math.ceil( math.abs(ri - rj) / 2.0 ) )
          val out = if(mi < mj) {
            0.0 //upper triangle: don't compute due to symmetry concerns
          } else if(D < -2.0) {
            val L = math.log((D / -2.0) + math.sqrt((D / -2.0) - 1.0) * math.sqrt((D / -2.0) + 1.0)) //acosh
            val hyperbolicFraction = if(n<500)
              (
                math.cosh( (n + 1 - math.abs((j+1)-(i+1))).toDouble * L ) - math.cosh( (n + 1 - (i+1) - (j+1)).toDouble * L )
              ) / math.sinh( ((n+1).toDouble) * L )
             else math.exp( (n + 1 - math.abs((j+1)-(i+1))).toDouble * L - ((n+1).toDouble) * L ) //asymptotic approximation to avoid overflow)
            val Rij = hyperbolicFraction / (
              -2.0 * math.sinh(L)
            ) / c
//            val Rij = (
//              math.cosh( (n + 1 - math.abs((j+1)-(i+1))).toDouble * L ) - math.cosh( (n + 1 - (i+1) - (j+1)).toDouble * L )
//            ) / (
//              -2.0 * math.sinh(L) * math.sinh( ((n+1).toDouble) * L )
//            ) / c
            Rij
          } else if (D > 2.0) {
            val L = math.log((D / 2.0) + math.sqrt((D / 2.0) - 1.0) * math.sqrt((D / 2.0) + 1.0)) //acosh
            val hyperbolicFraction = if(n<500)
              (
                math.cosh( (n + 1 - math.abs((j+1)-(i+1))).toDouble * L ) - math.cosh( (n + 1 - (i+1) - (j+1)).toDouble * L )
              ) / math.sinh( ((n+1).toDouble) * L )
             else math.exp( (n + 1 - math.abs((j+1)-(i+1))).toDouble * L - ((n+1).toDouble) * L ) //asymptotic approximation to avoid overflow)
            val Rij = hyperbolicFraction / (
              math.pow(-1.0, (i+j).toDouble ) * 2.0 * math.sinh(L)
            ) / c
//            val Rij = (
//              math.cosh( (n + 1 - math.abs((j+1)-(i+1))).toDouble * L ) - math.cosh( (n + 1 - (i+1) - (j+1)).toDouble * L )
//            ) / (
//              math.pow(-1.0, (i+j).toDouble ) * 2.0 * math.sinh(L) * math.sinh( ((n+1).toDouble) * L )
//            ) / c
            Rij
          } else { // -2.0 < D < 2.0
            val L = math.acos(D / 2.0) //no acosh for center case
            val Rij = (
              math.cos( (n + 1 - math.abs((j+1)-(i+1))).toDouble * L ) - math.cos( (n + 1 - (i+1) - (j+1)).toDouble * L )
            ) / (
              math.pow(-1.0, (i+j).toDouble ) * 2.0 * math.sin(L) * math.sin( ((n+1).toDouble) * L )
            ) / c
            Rij //never overflows because sin/cos are bounded
          }
          out
        }
      }

    val Hinvphi1nThetaDepRng = DenseVector.tabulate[Double](thetaDepRng.size){
      case i => if(thetaDepRng(i)==0 || thetaDepRng(i)==n-1)
        Hinvphi1nEdge
      else
        Hinvphi1nMiddle
    }

//      if(thetaDepRng.contains(0)) {
//
//    } else if(thetaDepRng.contains(n-1)) {
//      val v = DenseVector.ones[Double](thetaDepRng.size) :* Hinvphi1nMiddle
//    } else {
//      DenseVector.ones[Double](thetaDepRng.size) :* Hinvphi1nMiddle
//    }

    val covMatrix = symmetrizeFromLowerTriangle(R)
    //val oldCovMatrix = invertSymmetricMatrix( diag(DenseVector.ones[Double](n) :/ sigmasq) + (Hinvphi :/ tausq) )
    val meanVector = covMatrix * ( (data.y(thetaDepRng) :/ sigmasq) + (Hinvphi1nThetaDepRng :* (mu/tausq)) )

    val sidx = 0 until thetaRng.size
    val midx = (0 until thetaDepRng.size).diff(sidx)

    val x2 = theta(midx)

    val mu1 = meanVector(sidx)
    val mu2 = meanVector(midx)

    val Sigma11 = covMatrix(sidx,sidx)
    val Sigma22 = covMatrix(midx, midx)
    val Sigma12 = covMatrix(sidx, midx)
    val Sigma21 = covMatrix(midx, sidx)

    val Sigma22inv = try { invertSymmetricMatrix(Sigma22.toDenseMatrix) } catch {
      case e: Throwable => {
        println(
          s"""
            | b:$b c:$c D:$D
            | R:
            | $R
          """.stripMargin)
        throw e
      }
    }

    val mustar = mu1 + (((Sigma12 * Sigma22inv) * (x2 - mu2)).toDenseVector)
    val Sigmastar = Sigma11 - (((Sigma12 * Sigma22inv) * Sigma21).toDenseMatrix)

    val out = try { MultivariateGaussian(mustar, symmetrizeFromLowerTriangle(Sigmastar)).draw() } catch {
      case e: Throwable => {
        println(
          s"""
             | Sigmastar:
             | $Sigmastar
          """.stripMargin)
        throw e
      }

    }
    out
  }

  def updateSigmasq(latest: ConcurrentHashMap[GPRVariables.anyVar, Any]) = {
    val y = data.y
    val theta = getTheta(latest)
    val n = dims.n
    val aSigma = tuningParameters.aSigma
    val bSigma = tuningParameters.bSigma
    val sumSqYTheta = selfInnerProduct(y - theta)

    val alpha = 0.5 * n.toDouble + aSigma
    val beta = bSigma + 0.5 * sumSqYTheta

    val out = rinvgamma(alpha, beta)
    out
  }

  def updateMu(latest: ConcurrentHashMap[GPRVariables.anyVar, Any]) = {
    val tausq = latest.get(GPRVariables.tausq).asInstanceOf[Double]
    val aMu = tuningParameters.aMu
    val bMu = tuningParameters.bMu
    val theta = getTheta(latest)
    //val Hinvphi1n = fixedMatrices.Hinvphi1n
    val n = dims.n
    val Hinvphi1nEdge = fixedMatrices.Hinvphi1nEdge
    val Hinvphi1nMiddle = fixedMatrices.Hinvphi1nMiddle
    val t1nHinvphi1n = fixedMatrices.t1nHinvphi1n

    val sumThetaHinvphi1n = theta(0) * Hinvphi1nEdge + sum(theta(1 until n-1) :* Hinvphi1nMiddle) + theta(n-1) * Hinvphi1nEdge

    val mean = (tausq * aMu + bMu * sumThetaHinvphi1n) / (tausq + bMu * t1nHinvphi1n)
    val variance = (tausq * bMu) / (tausq + bMu * t1nHinvphi1n)

    val out = Gaussian(mean, math.sqrt(variance)).draw()
    out
  }

  def updateTausq(latest: ConcurrentHashMap[GPRVariables.anyVar, Any]) = {
    val n = dims.n
    val aTau = tuningParameters.aTau
    val bTau = tuningParameters.bTau
    val theta = getTheta(latest)
    val mu = latest.get(GPRVariables.mu).asInstanceOf[Double]
    val Hinvphi = fixedMatrices.Hinvphi

    val alpha = 0.5 * n.toDouble + aTau
    val beta = bTau + 0.5 * quadraticProduct((theta :- mu), Hinvphi)

    val out = rinvgamma(alpha, beta)
    out
  }






  def generateData(): (DenseVector[Double], DenseVector[Double]) = {
    println("generating data")
    val replicableRand = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(1)))
    val x = new DenseVector[Double]((dims.min until dims.max by dims.gridsize).toArray)
    val sd = 0.2
    val y = x.map{
      x => {
        val mx = ( ((((x + 3.0) % 6.0) + 6.0) % 6.0) - 3.0) * math.pow(-1.0, math.floor((x + 3.0) / 6.0))
        val fx = 0.3 + 0.4 * mx + 0.5 * math.sin(2.7 * mx) + (1.1 / (1.0 + mx*mx))
        val eps = Gaussian(0.0,sd)(replicableRand).draw()
        val y = fx + eps
        y
      }
    }
    println(s"loaded data of size ${x.length}")
    (x,y)
  }
}
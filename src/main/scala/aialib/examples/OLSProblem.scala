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

import java.util.concurrent.atomic.{AtomicReference, AtomicReferenceArray, AtomicLongArray, AtomicLong}
import java.util.function.{LongBinaryOperator, BinaryOperator, UnaryOperator}
import aialib.{Cluster, Problem}
import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.file.{Files, Paths}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import breeze.linalg.CSCMatrix._
import breeze.linalg.DenseMatrix._
import breeze.linalg.DenseVector._
import breeze.linalg.SparseVector._
import breeze.linalg._
import breeze.stats.distributions._
import org.apache.commons.math3.random.MersenneTwister
import scala.collection.JavaConversions._
import scala.collection.parallel.ForkJoinTaskSupport

/*
* warning:
*
* variables used inside here may not conform to scala naming conventions
* because they are named according to mathematical conventions which
* distinguish between upper and lower-case greek letters
*
* please do not alter this naming style
*/


object OLSProblem extends App {
  val hostname = args(0)
  val port = args(1)
  val controlURL = args(2)
  new Cluster[OLSProblem,OLSVariables.anyVar](hostname, port, controlURL)
}

object OLSVariables {
  sealed trait anyVar
  case class beta(id: Int) extends anyVar
  case object mu extends anyVar
  case object Sigmainv extends anyVar
  case object gamma extends anyVar
  case object nuinv extends anyVar
}

class OLSProblem extends Problem[OLSVariables.anyVar] with Sampling {
  def nMC: Long = 12500000
  override def numberOfMachines = nMachines //used with lazy evaluation to avoid loading data on control
  def nThreadsPerMachine = 8
  def dataSize = 1000000 //(10 000 000 samples / 1 000 000 vars) * 100 threads = 1000 samples/var
  def nMachines = 12

//  def nMC: Long = 100000
//  def nMachines = 2
//  override def numberOfMachines = nMachines
//  def nThreadsPerMachine = 4
//  def dataSize = 10000

  def nWorkers = nMachines * nThreadsPerMachine
  def workloadMap = (0 until nMachines)
    .map(
      workerId => {
        val port = workerId+2552
        val varsPerThread = (0 until nThreadsPerMachine)
          .map(threadId => ourVariableIds(workerId, threadId))
        (port, varsPerThread)
      }
    )
    .toMap
  def ourVariableIds(workerId: Int, threadId: Int): Set[OLSVariables.anyVar] = {
    val uniqueId = workerId * nThreadsPerMachine + threadId
    val betas = (uniqueId*(dims.n/nWorkers) until (uniqueId+1)*(dims.n/nWorkers)).map(id => OLSVariables.beta(id)).toSet
    Set.empty[OLSVariables.anyVar] ++ betas + OLSVariables.mu + OLSVariables.gamma + OLSVariables.nuinv + OLSVariables.Sigmainv
  }




  def setRandomNumberSeed(workerId: Int) = Rand.generator.setSeed(workerId * 1000000) //really bad solution: change later




  lazy val dataWithInitialAndCache = readInData(dataSize)
  lazy val data = dataWithInitialAndCache.map(d => (d._1, d._2))

  def thinning(variable: OLSVariables.anyVar) = variable match {
    case OLSVariables.beta(id: Int) =>
      if(Uniform(0,1).draw() < 0.1) {
        true
      } else {
        false
      }
    case _ =>
      false
  }

  object files {
    val y = "data/Y.txt"
    val f = "data/F.txt"
    val dataSerialized = "data/data.ser"
  }

  lazy val storedBetas = listOfBetas.map(v => v._2.head.head).toSet
  def storedVariables: Set[OLSVariables.anyVar] = {
    Set.empty[OLSVariables.anyVar] ++
      storedBetas +
    // (0 until dims.n).map(id => OLSVariables.beta(id)).toSet +
      OLSVariables.mu + OLSVariables.gamma +
      OLSVariables.nuinv // + MobileAppVariables.Sigmainv
  }

  object dims {
    lazy val n = data.length
    lazy val d = data.head._2.cols
    lazy val TmP = data.head._1.length
  }
  object fixedMatrices {
    lazy val Wi = diag(DenseVector.ones[Double](dims.TmP))
    lazy val WtW = selfInnerMatrixProduct(Wi) :* dims.n.toDouble
  }
  object tuningParameters {
    val kappagamma = 1000000.0
    val kappamu = 1000000.0
    val eps = 0.001
    lazy val kappagammaIinv = diag(DenseVector.ones[Double](dims.TmP)) * (1.0/kappagamma)
    lazy val kappamuIinv = diag(DenseVector.ones[Double](dims.d)) * (1.0/kappamu)
  }




  def getInitial() = {
    println("drawing initial values")
    val replicableRand = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(1)))

//    val mu = MultivariateGaussian(
//      DenseVector.zeros[Double](dims.d),
//      diag(DenseVector.ones[Double](dims.d))
//    )(replicableRand).draw()
    val mu = DenseVector.zeros[Double](dims.d)
    val Sigmainv = diag(DenseVector.ones[Double](dims.d))
//    val gamma = MultivariateGaussian(
//      DenseVector.zeros[Double](dims.TmP),
//      diag(DenseVector.ones[Double](dims.TmP))
//    )(replicableRand).draw()
    val gamma = DenseVector.zeros[Double](dims.TmP)
    val nuinv = 1.0


    val beta = dataWithInitialAndCache.map(d => d._3)

//    val latestExceptBeta = new ConcurrentHashMap(
//      mapAsJavaMap(
//        (0 until dims.n).map(
//          betaIdx => (OLSVariables.beta(betaIdx), DenseVector.zeros[Double](dims.d))
//        ).toMap ++ Map[OLSVariables.anyVar,Any](
//      OLSVariables.mu -> mu, OLSVariables.Sigmainv -> Sigmainv,
//      OLSVariables.gamma -> gamma, OLSVariables.nuinv -> nuinv)
//      )
//    )
//    val beta = (0 until dims.n).map{
//      betaIdx => {
//        if(betaIdx % math.ceil(0.05 * dims.n.toDouble).toInt == 0) {
//          println(s"drew initial beta: $betaIdx/${dims.n}")
//        }
//        updateBeta(betaIdx, latestExceptBeta)(replicableRand)
//      }
//    }.toArray

    val out = (0 until dims.n).map{
      betaIdx => (OLSVariables.beta(betaIdx), beta(betaIdx))
    }.toMap ++ Map[OLSVariables.anyVar, Any](
      OLSVariables.mu -> mu,
      OLSVariables.Sigmainv -> Sigmainv,
      OLSVariables.gamma -> gamma,
      OLSVariables.nuinv -> nuinv
    )

    out
  }




  lazy val listOfBetas = workloadMap.map{
    case (key, arrayOfSets) => {
      val arrayOfVecs = arrayOfSets.map{
        set => set
          .filter{
            case v: OLSVariables.beta => true
            case _ => false
          }
          .toVector
      }
      (key, arrayOfVecs)
    }
  }
  def selectNextVariable(workerId: Int, threadId: Int) = {
    val possibleBetas = listOfBetas
      .apply(workerId)
      .apply(threadId)

    val percentageBetas = possibleBetas.size.toDouble / (possibleBetas.size.toDouble + 4.0)
    val percentageNonBetas = 1.0 - percentageBetas
    val probabilityNonBetas = percentageNonBetas / nThreadsPerMachine.toDouble
    val probabilityBetas = 1.0 - probabilityNonBetas

    val out = if(Uniform(0,1).draw() < probabilityBetas) {
      val randomIdx = scala.util.Random.nextInt(possibleBetas.size)
      possibleBetas(randomIdx)
    } else {
      val randomIdx = scala.util.Random.nextInt(4)
      if(randomIdx == 0) OLSVariables.mu
      else if(randomIdx == 1) OLSVariables.Sigmainv
      else if(randomIdx == 2) OLSVariables.gamma
      else OLSVariables.nuinv
    }
    out
  }

  def update(workerId: Int, threadId: Int,
             latest: ConcurrentHashMap[OLSVariables.anyVar, Any]
              ): (OLSVariables.anyVar, Any) = selectNextVariable(workerId, threadId)  match {
    case v@OLSVariables.beta(betaIdx) => {
      val newBeta = updateBeta(betaIdx, latest)
      (v, newBeta)
    }

    case v@OLSVariables.mu => {
      val newMu = updateMu(latest)
      (v, newMu)
    }

    case v@OLSVariables.Sigmainv => {
      val newSigmainv = updateSigmainv(latest)
      (v, newSigmainv)
    }
    case v@OLSVariables.gamma => {

      val newGamma = updateGamma(latest)
      (v, newGamma)
    }
    case v@OLSVariables.nuinv => {
      val newNuinv = updateNuinv(latest)
      (v, newNuinv)
    }
  }






  class AtomicDoubleArray(startValues: Array[Double]) {
    private val underlyingLongArray = new AtomicLongArray(startValues.map(
      dbl => java.lang.Double.doubleToLongBits(dbl))
    )
    def get(idx: Int): Double = {
      val outLng = underlyingLongArray.get(idx)
      val outDbl = java.lang.Double.longBitsToDouble(outLng)
      outDbl
    }
    def getAndSet(idx: Int, inDbl: Double) = {
      val inLng = java.lang.Double.doubleToLongBits(inDbl)
      val outLng = underlyingLongArray.getAndSet(idx, inLng)
      val outDbl = java.lang.Double.longBitsToDouble(outLng)
      outDbl
    }
  }
  class AtomicDouble(startValue: Double = 0.0) {
    private val underlyingLong = new AtomicLong(java.lang.Double.doubleToLongBits(startValue))
    def get(): Double = {
      val outLng = underlyingLong.get()
      val outDbl = java.lang.Double.longBitsToDouble(outLng)
      outDbl
    }
    def addAndGet(dblDelta: Double) = {
      val dblDeltaAsLong = java.lang.Double.doubleToLongBits(dblDelta)
      val outLng = underlyingLong.accumulateAndGet(dblDeltaAsLong, addDblAsLongOp)
      val outDbl = java.lang.Double.longBitsToDouble(outLng)
      outDbl
    }
  }
  lazy val cacheValuesWxYmFB = {
    println("initializing WxYmFB cache")
//    val latest = getInitial()
//    val beta = getBeta(latest)
//    val Wi = fixedMatrices.Wi
//
//    val WxYmFB = data.zip(beta).map{
//      case ((yi, fi),betai) => {
//        val out = (Wi * (yi - (fi * betai)))
//        out
//      }
//    }
    val WxYmFB = dataWithInitialAndCache.map(d => d._4)
    new AtomicReferenceArray(WxYmFB)
  }
  lazy val cacheValuesSDiff = {
    println("initializing SDiff cache")
//    val latest = getInitial()
//    val beta = getBeta(latest)
//    val mu = latest.apply(OLSVariables.mu).asInstanceOf[DenseVector[Double]]
//
//    val SDiff = beta.map{
//      betai => {
//        val out = betai - mu
//        out
//      }
//    }
    val SDiff = dataWithInitialAndCache.map(d => d._5)
    new AtomicReferenceArray(SDiff)
  }
  lazy val cacheValuesLargeSum = {
    println("initializing largeSum cache")
//    val latest = getInitial()
//    val gamma = latest.apply(OLSVariables.gamma).asInstanceOf[DenseVector[Double]]
//    val beta = getBeta(latest)
//
//    val Wi = fixedMatrices.Wi
//
//    val largeSum = data.zip(beta).map{
//      case ((yi, fi), betai) => {
//        val diff = yi - (fi * betai) - (Wi * gamma)
//        val out = selfInnerProduct(diff)
//        out
//      }
//    }
    val largeSum = dataWithInitialAndCache.map(d => d._6)
    new AtomicDoubleArray(largeSum)
  }
  lazy val cacheWxYmFB = {
    val start = (0 until dims.n).map(
      idx => cacheValuesWxYmFB.get(idx)
    ).reduce((v1,v2) => v1 :+ v2)
    new AtomicReference(start)
  }
  lazy val cacheS = {
    val start = (0 until dims.n).map(
      idx => selfOuterProduct(cacheValuesSDiff.get(idx))
    ).reduce((v1,v2) => v1 :+ v2)
    new AtomicReference(start)
  }
  lazy val cacheLargeSum = {
    val start = (0 until dims.n).map(
      idx => cacheValuesLargeSum.get(idx)
    ).sum
    new AtomicDouble(start)
  }
  lazy val cacheBetaSum = {
    println("initializing betaSum cache")
    val beta = dataWithInitialAndCache.map(d => d._3)
    val start = beta.reduce((v1, v2) => v1 :+ v2)
    new AtomicReference(start)
  }
  val vecSumOp = new BinaryOperator[DenseVector[Double]] {
    def apply(u: DenseVector[Double], v: DenseVector[Double]) = u :+ v
  }
  val matSumOp = new BinaryOperator[DenseMatrix[Double]] {
    def apply(u: DenseMatrix[Double], v: DenseMatrix[Double]) = u :+ v
  }
  val addDblAsLongOp = new LongBinaryOperator {
    def applyAsLong(uLng: Long, vLng: Long) = {
      val uDbl = java.lang.Double.longBitsToDouble(uLng)
      val vDbl = java.lang.Double.longBitsToDouble(vLng)
      val outDbl = uDbl + vDbl
      val outLng = java.lang.Double.doubleToLongBits(outDbl)
      outLng
    }
  }
  override def processUpdate(variableId: OLSVariables.anyVar, newValue: Any, oldValue: Any,
                             latest: ConcurrentHashMap[OLSVariables.anyVar, Any]): Unit = variableId match {
    case v@OLSVariables.beta(betaIdx) => {
      val (yi, fi) = data(betaIdx)
      val Wi = fixedMatrices.Wi
      val betai = newValue.asInstanceOf[DenseVector[Double]]
      val mu = latest.get(OLSVariables.mu).asInstanceOf[DenseVector[Double]]
      val gamma = latest.get(OLSVariables.gamma).asInstanceOf[DenseVector[Double]]

      val newWxYmFB = (Wi * (yi - (fi * betai)))
      val oldWxYmFB = cacheValuesWxYmFB.getAndSet(betaIdx, newWxYmFB)
      val diffWxYmFB = newWxYmFB :- oldWxYmFB
      val newCacheWxYmFB = cacheWxYmFB.accumulateAndGet(diffWxYmFB, vecSumOp)

      val newSDiff = betai - mu
      val oldSDiff = cacheValuesSDiff.getAndSet(betaIdx, newSDiff)
      val diffS = selfOuterProduct(newSDiff) :- selfOuterProduct(oldSDiff)
      val newCacheS = cacheS.accumulateAndGet(diffS, matSumOp)

      val newLargeSum = selfInnerProduct(yi - (fi * betai) - (Wi * gamma))
      val oldLargeSum = cacheValuesLargeSum.getAndSet(betaIdx, newLargeSum)
      val diffLargeSum = newLargeSum - oldLargeSum
      val newCacheLargeSum = cacheLargeSum.addAndGet(diffLargeSum)

      val oldBetai = oldValue.asInstanceOf[DenseVector[Double]]
      val diffBetaSum = betai - oldBetai
      val newCacheBetaSum = cacheBetaSum.accumulateAndGet(diffBetaSum, vecSumOp)
    }
    case _ => ;
  }






  def getBeta(latest: ConcurrentHashMap[OLSVariables.anyVar, Any]) = {
    val currentBetas = (0 until dims.n).toArray.map{
      betaIdx => latest.get(OLSVariables.beta(betaIdx)).asInstanceOf[DenseVector[Double]]
    }
    currentBetas
  }
  def getBeta(latest: Map[OLSVariables.anyVar, Any]) = {
    val currentBetas = (0 until dims.n).toArray.map{
      betaIdx => latest.apply(OLSVariables.beta(betaIdx)).asInstanceOf[DenseVector[Double]]
    }
    currentBetas
  }

  def updateBeta(betaIdx: Int, latest: ConcurrentHashMap[OLSVariables.anyVar, Any])
                (implicit rand: RandBasis = Rand): DenseVector[Double] = {
    val mu = latest.get(OLSVariables.mu).asInstanceOf[DenseVector[Double]]
    val Sigmainv = latest.get(OLSVariables.Sigmainv).asInstanceOf[DenseMatrix[Double]]
    val gamma = latest.get(OLSVariables.gamma).asInstanceOf[DenseVector[Double]]
    val nuinv = latest.get(OLSVariables.nuinv).asInstanceOf[Double]

    val datai = data(betaIdx)
    val yi = datai._1
    val fi = datai._2
    val Wi = fixedMatrices.Wi

    val C = invertSymmetricMatrix(Sigmainv + (selfInnerMatrixProduct(fi) :* nuinv))
    val m = C * ( ( (fi.t * (yi - (Wi * gamma))) :* nuinv ) + (Sigmainv * mu) )

    val out = MultivariateGaussian(m,C)(rand).draw()
    out
  }

  def updateMu(latest: ConcurrentHashMap[OLSVariables.anyVar, Any]): DenseVector[Double] = {
    val beta = getBeta(latest)
    val Sigmainv = latest.get(OLSVariables.Sigmainv).asInstanceOf[DenseMatrix[Double]]

    val n = dims.n

    //val betabar = beta.reduce((a,b) => a+b) / (n.toDouble)
    val betabar = cacheBetaSum.get() / (n.toDouble)

//    if(Uniform(0,1).draw() < 0.1) {
//      val betaSum = beta.reduce((a,b) => a+b)
//      println(s"betaSum cached: ${betabar(0) * n.toDouble}, actual betaSum: ${betaSum(0)}")
//    }

    val kappamuIinv = tuningParameters.kappamuIinv

    val B: DenseMatrix[Double] = invertSymmetricMatrix(kappamuIinv + (Sigmainv :* n.toDouble))
    val a: DenseVector[Double] = B * ((Sigmainv * betabar) :* n.toDouble)

    val out = MultivariateGaussian(a,B).draw()
    out
  }

  def updateSigmainv(latest: ConcurrentHashMap[OLSVariables.anyVar, Any]): DenseMatrix[Double] = {
    val beta = getBeta(latest)
    val mu = latest.get(OLSVariables.mu).asInstanceOf[DenseVector[Double]]

    val n = dims.n
    val d = dims.d

//    val S = beta.foldLeft {
//      DenseMatrix.zeros[Double](d,d)
//    }{
//      (currentValue, betai) => {
//        val diff = betai - mu
//        val next = currentValue + selfOuterProduct(diff)
//        next
//      }
//    }
    val S = cacheS.get()

//    if(Uniform(0,1).draw() < 0.1) {
//      val actualS = beta.foldLeft {
//        DenseMatrix.zeros[Double](d,d)
//      }{
//        (currentValue, betai) => {
//          val diff = betai - mu
//          val next = currentValue + selfOuterProduct(diff)
//          next
//        }
//      }
//      println(s"S cached: ${S(0,0)}, actual: ${actualS(0,0)}")
//    }

    val Psi = invertSymmetricMatrix(S + DenseMatrix.eye[Double](S.cols))
    val df = (n+d+1).toDouble

    val out = rwish(df, Psi)
    out
  }

  def updateGamma(latest: ConcurrentHashMap[OLSVariables.anyVar, Any]): DenseVector[Double] = {
    val nuinv = latest.get(OLSVariables.nuinv).asInstanceOf[Double]
    val beta = getBeta(latest)

    val n = dims.n
    val TmP = dims.TmP

    val Wi = fixedMatrices.Wi
    val WtW = fixedMatrices.WtW

//    val WxYmFB = data.zip(beta).foldLeft{
//      DenseVector.zeros[Double](TmP)
//    }{
//      case (currentValue, ((yi, fi),betai)) => {
//        val next = currentValue + (Wi * (yi - (fi * betai)))
//        next
//      }
//    }
    val WxYmFB = cacheWxYmFB.get()


//    if(Uniform(0,1).draw() < 0.1) {
//      val actualWxYmFB = data.zip(beta).foldLeft{
//        DenseVector.zeros[Double](TmP)
//      }{
//        case (currentValue, ((yi, fi),betai)) => {
//          val next = currentValue + (Wi * (yi - (fi * betai)))
//          next
//        }
//      }
//      println(s"WxYmFB cached: ${WxYmFB(0)}, actual: ${actualWxYmFB(0)}")
//    }

    val kappagammaIinv = tuningParameters.kappagammaIinv

    val D = invertSymmetricMatrix(kappagammaIinv + (WtW :* nuinv))
    val c = (D * WxYmFB) :* nuinv

    val out = MultivariateGaussian(c,D).draw()
    out
  }

  def updateNuinv(latest: ConcurrentHashMap[OLSVariables.anyVar, Any]): Double = {
    val gamma = latest.get(OLSVariables.gamma).asInstanceOf[DenseVector[Double]]
    val beta = getBeta(latest)

    val Wi = fixedMatrices.Wi

    val n = dims.n
    val TmP = dims.TmP
    val eps = tuningParameters.eps

//    val largeSum = data.zip(beta).foldLeft{
//      0.0
//    }{
//      case (currentValue, ((yi, fi), betai)) => {
//        val diff = yi - (fi * betai) - (Wi * gamma)
//        val next = selfInnerProduct(diff) + currentValue
//        next
//      }
//    }
    val largeSum = cacheLargeSum.get()



//    if(Uniform(0,1).draw() < 0.1) {
//      val actualLargeSum = data.zip(beta).foldLeft{
//        0.0
//      }{
//        case (currentValue, ((yi, fi), betai)) => {
//          val diff = yi - (fi * betai) - (Wi * gamma)
//          val next = selfInnerProduct(diff) + currentValue
//          next
//        }
//      }
//      println(s"largeSum cached: ${largeSum}, actual: ${actualLargeSum}")
//    }

    val k = (eps + ((n * TmP).toDouble)) / 2.0
    val theta = 1.0 / ( (eps + largeSum)/2.0 )

    val out = Gamma(k, theta).draw()
    out
  }






  def readInData(dataSize: Int) = {
    println("loading data")
    println(s"java max memory: ${Runtime.getRuntime().maxMemory() / 1024 / 1024 / 1024}G")

    if( !Files.exists(Paths.get(files.dataSerialized)) ) {
      println(s"reading from ${files.y}")
      val y = scala.io.Source.fromFile(files.y)
        .getLines().map(_.toDouble).grouped(51)
        .map{
          values => values.zipWithIndex
        }.map {
        pairs => pairs.filter(pair => pair._1 != 0.0)
      }.map {
        pairs => pairs.map{pair => (pair._2, pair._1)}
      }.map {
        list => SparseVector(51)(list: _*)
      }.toIterable

      println(s"reading from ${files.f}")
      val f = scala.io.Source.fromFile(files.f)
        .getLines().map {
        _.split(" ").map(_.toDouble).zipWithIndex
      }.grouped(51).map{
        group => {
          val builder = new CSCMatrix.Builder[Double](rows=51, cols=30)
          group.zipWithIndex.foreach{
            case (rowDataWithColIdx, rowIdx) => rowDataWithColIdx.foreach{
              case (value, colIdx) => {
                if(value != 0.0F) builder.add(rowIdx,colIdx,value)
              }
            }
          }
          builder.result()
        }
      }.toIterable


      val replicableRand = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(1)))
      val mu = DenseVector.zeros[Double](30)
      val Sigmainv = diag(DenseVector.ones[Double](30))
      val gamma = DenseVector.zeros[Double](51)
      val Wi = diag(DenseVector.ones[Double](51))
      val nuinv = 1.0

      val beta = y.zip(f).zipWithIndex.map{
        case ((yi,fi), betaIdx) => {
          if (betaIdx % math.ceil(0.05 * dataSize.toDouble).toInt == 0) {
            println(s"drew initial beta: $betaIdx/$dataSize")
          }

          val C = invertSymmetricMatrix(Sigmainv + (selfInnerMatrixProduct(fi) :* nuinv))
          val m = C * ( ( (fi.t * (yi - (Wi * gamma))) :* nuinv ) + (Sigmainv * mu) )

          val out = MultivariateGaussian(m,C)(replicableRand).draw()
          out
        }
      }

      val dataWithInitial = y.zip(f).zip(beta).map{
        case ((y, f), beta) => (y,f,beta)
      }.toArray



      val WxYmFB = dataWithInitial.map{
        case (yi, fi, betai) => {
          val out = (Wi * (yi - (fi * betai)))
          out
        }
      }
      val SDiff = dataWithInitial.map(d => d._3).map{
        betai => {
          val out = betai - mu
          out
        }
      }
      val largeSum = dataWithInitial.map{
        case (yi, fi, betai) => {
          val diff = yi - (fi * betai) - (Wi * gamma)
          val out = selfInnerProduct(diff)
          out
        }
      }

      val in = dataWithInitial.zip(WxYmFB).zip(SDiff).zip(largeSum)
        .map(
          d => (d._1._1._1._1,d._1._1._1._2,d._1._1._1._3,
                d._1._1._2, d._1._2, d._2)
        )

      val out = in.take(dataSize)

      println(s"read in data of size ${in.length}, writing first ${out.length} points to disk")

      val writer = new ObjectOutputStream(new FileOutputStream(files.dataSerialized))
      writer.writeObject(out)
      writer.close()

      println(s"finished writing")
    }

    println(s"reading in from ${files.dataSerialized}")
    val reader = new ObjectInputStream(new FileInputStream(files.dataSerialized))
    val in = reader.readObject()
      .asInstanceOf[Array[(SparseVector[Double], CSCMatrix[Double], DenseVector[Double],
        DenseVector[Double], DenseVector[Double], Double
      )]]

    val out = in.take(dataSize)
    println(s"read in data of size ${in.length}, loaded first ${out.length} points")

    out
  }
}

object OLSSequentialScan extends App {
  val nMC = 1000
  val nThreads = 8
  val seed = 1

  println("loading data")

  val problem = new OLSProblem
  problem.setRandomNumberSeed(1)
  val data = problem.data
  val dims = problem.dims
  val tuningParameters = problem.tuningParameters
  val fixedMatrices = problem.fixedMatrices

  val storedVariables = Set.empty[OLSVariables.anyVar] ++
    (0 until 2).map(id => OLSVariables.beta(id)).toSet +
    OLSVariables.mu + OLSVariables.gamma +
    OLSVariables.nuinv // + MobileAppVariables.Sigmainv

  val ourVariableIds = {
    val betas = (0 until dims.n).map(id => OLSVariables.beta(id)).toSet
    Set.empty[OLSVariables.anyVar] ++ betas + OLSVariables.mu + OLSVariables.gamma + OLSVariables.nuinv + OLSVariables.Sigmainv
  }

  val out = ourVariableIds.intersect(storedVariables).map(_ -> new ConcurrentLinkedQueue[Any]()).toMap

  val betaParRange = (0 until dims.n).par
  betaParRange.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(nThreads))

  println("drawing initial values")

  val latest = new ConcurrentHashMap[OLSVariables.anyVar, Any](mapAsJavaMap(problem.getInitial))

  println("starting MCMC")

  for(i <- 1 to nMC) {
    //update beta
    for(betaIdx <- betaParRange){
      val newBeta = problem.updateBeta(betaIdx, latest)
      val oldBeta = latest.replace(OLSVariables.beta(betaIdx), newBeta)
      problem.processUpdate(OLSVariables.beta(betaIdx), newBeta, oldBeta, latest)

      if (storedVariables.contains(OLSVariables.beta(betaIdx))){
        out(OLSVariables.beta(betaIdx)).add(newBeta)
      }

//      if (betaIdx % (dims.n / 5) == 0) {
//        println(s"total beta iterations: $betaIdx")
//      }
    }


    //update mu
    val newMu = problem.updateMu(latest)
    val oldMu = latest.replace(OLSVariables.mu, newMu)
    problem.processUpdate(OLSVariables.mu, newMu, oldMu, latest)

    if (storedVariables.contains(OLSVariables.mu)){
      out(OLSVariables.mu).add(newMu)
    }


    //update Sigmainv
    val newSigmainv = problem.updateSigmainv(latest)
    val oldSigmainv = latest.replace(OLSVariables.Sigmainv, newSigmainv)
    problem.processUpdate(OLSVariables.Sigmainv, newSigmainv, oldSigmainv, latest)

    if (storedVariables.contains(OLSVariables.Sigmainv)){
      out(OLSVariables.Sigmainv).add(newSigmainv)
    }


    //update gamma
    val newGamma = problem.updateGamma(latest)
    val oldGamma = latest.replace(OLSVariables.gamma, newGamma)
    problem.processUpdate(OLSVariables.gamma, newGamma, oldGamma, latest)

    if (storedVariables.contains(OLSVariables.gamma)){
      out(OLSVariables.gamma).add(newGamma)
    }


    //update nuinv
    val newNuinv = problem.updateNuinv(latest)
    val oldNuinv = latest.replace(OLSVariables.nuinv, newNuinv)
    problem.processUpdate(OLSVariables.nuinv, newNuinv, oldNuinv, latest)

    if (storedVariables.contains(OLSVariables.nuinv)){
      out(OLSVariables.nuinv).add(newNuinv)
    }

    //print status
    if (i % 1 == 0) {
      println(s"total samples: $i")
    }
  }

  //print results
  aialib.Worker.writeOutput("sequential",out)
}



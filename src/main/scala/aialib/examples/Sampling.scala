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

import breeze.linalg._
import breeze.stats.distributions._
import com.github.fommil.netlib.LAPACK.{getInstance => lapack}
import org.netlib.util.intW


trait Sampling {
  def rwish(df: Double, Psi: DenseMatrix[Double])(implicit rand: RandBasis = Rand): DenseMatrix[Double] = {
    /*
    * this function adopts the Odell & Feiveson (1966) routine
    * for drawing Wishart samples via the Bartlett decomposition,
    * which amounts to generating standard normal and chi-square
    * random variables, arranging them into a lower triangular
    * matrix, and multiplying different things around
    */
    val dim = Psi.rows
    if(dim != Psi.cols) throw new IllegalArgumentException(
      s"Mismatched matrix dimensions in rwish: expected Psi square, actual Psi (${Psi.rows},${Psi.cols})"
    )
    if(!(df > dim.toDouble - 1.0)) throw new IllegalArgumentException(
      s"Undefined distribution in rwish: df cannot be less than the size of Psi"
    )

    val L: DenseMatrix[Double] = cholesky(Psi)
    val A: DenseMatrix[Double] = DenseMatrix.tabulate(dim,dim){
      case (i,j) => {
        if(i == j){
          val chisq = ChiSquared(df-i)(rand).draw()
          breeze.numerics.sqrt(chisq)
        }else if(i > j){
          Gaussian(0,1)(rand).draw()
        }else{
          0.0
        }
      }
    }


    val out: DenseMatrix[Double] = selfOuterMatrixProduct(L * A)
    out
  }

  def invertSymmetricMatrix(M: DenseMatrix[Double]): DenseMatrix[Double] = {
    val L = {
      try {
        cholesky(M) //note: Cholesky already checks for symmetry and squareness
      } catch {
        case e: MatrixNotSymmetricException =>
          println("WARNING: nonsymmetric matrix in invertSymmetric matrix, using lower triangular portion")
          cholesky(symmetrizeFromLowerTriangle(M))
      }
    }

    val uplo = "L"
    val N = L.rows
    val LDA  = scala.math.max(1,N)
    val info = new intW(0)
    lapack.dpotri(uplo, N, L.data, LDA, info)
    assert(info.`val` >= 0, "Malformed argument %d (LAPACK)".format(-info.`val`))
    if (info.`val` > 0)
      throw new IllegalArgumentException(
        s"Singular matrix in invertSymmetricMatrix: inverse does not exist"
      )
    val Minv = L + strictlyUpperTriangular(L.t)
    Minv
  }

  def innerProduct(u: DenseVector[Double], v: DenseVector[Double]) = {
    if (u.length != v.length) {
      throw new IllegalArgumentException(s"Mismatched vector dimensions in sumSq: actual ${v.length} != ${u.length}")
    }
    val utv = sum(u :* v) //same as u.t * v
    utv
  }

  def outerProduct(u: DenseVector[Double], v: DenseVector[Double]) = {
    if (u.length != v.length) {
      throw new IllegalArgumentException(s"Mismatched vector dimensions in sumSq: actual ${u.length} != ${v.length}")
    }
    val uvt = u * v.t
    uvt
  }

  def quadraticProduct(v: DenseVector[Double], M: DenseMatrix[Double]) = {
    if (M.cols != M.rows) {
      throw new IllegalArgumentException(s"Nonsquare matrix in quadraticProduct, actual (${M.rows},${M.cols})")
    } else if (M.cols != v.length) {
      throw new IllegalArgumentException(s"Mismatched matrix dimensions in quadraticProduct, actual (${v.length}), (${M.rows},${M.cols})")
    }
    val Mv = M * v
    val vtMv = sum(v :* Mv) //same as v.t * Mv
    vtMv
  }

  def quadraticProduct(v: DenseVector[Double], M: CSCMatrix[Double]) = {
    if (M.cols != M.rows) {
      throw new IllegalArgumentException(s"Nonsquare matrix in quadraticProduct, actual (${M.rows},${M.cols})")
    } else if (M.cols != v.length) {
      throw new IllegalArgumentException(s"Mismatched matrix dimensions in quadraticProduct, actual (${v.length}), (${M.rows},${M.cols})")
    }
    val Mv = M * v
    val vtMv = sum(v :* Mv) //same as v.t * Mv
    vtMv
  }

  def selfInnerProduct(v: DenseVector[Double]) = {
    sum(v.map(d => d * d))
  }

  def selfOuterProduct(v: DenseVector[Double]) = {
    outerProduct(v,v)
  }

  def selfOuterMatrixProduct(M: DenseMatrix[Double]): DenseMatrix[Double] = {
    M * M.t
  }

  def selfInnerMatrixProduct(M: DenseMatrix[Double]): DenseMatrix[Double] = {
    M.t * M
  }

  def selfInnerMatrixProduct(M: CSCMatrix[Double]): CSCMatrix[Double] = {
    M.t * M
  }

  def rinvgamma(alpha: Double, beta: Double)(implicit rand: RandBasis = Rand) = {
    1.0/Gamma(alpha, 1.0/beta)(rand).draw()
  }

  def symmetrizeFromLowerTriangle(M: DenseMatrix[Double]): DenseMatrix[Double] = {
    if (M.cols != M.rows) {
      throw new IllegalArgumentException(s"Nonsquare matrix in symmetrizeFromLowerTriangle, actual (${M.rows},${M.cols})")
    }
    lowerTriangular(M) + (strictlyLowerTriangular(M).t)
  }
}
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

trait Problem[V] {
  def update(workerId: Int, threadId: Int, latest: java.util.concurrent.ConcurrentHashMap[V, Any]): (V, Any)
  def nMC: Long
  def getInitial: Map[V, Any]
  def storedVariables: Set[V]
  def thinning(variable: V): Boolean
  def setRandomNumberSeed(workerId: Int): Unit
  def workloadMap: Map[Int, IndexedSeq[Set[V]]]
  def numberOfMachines: Int = workloadMap.size
  def processUpdate(variableId: V, newValue: Any, oldValue: Any,
                    latest: java.util.concurrent.ConcurrentHashMap[V, Any]): Unit = Unit
}
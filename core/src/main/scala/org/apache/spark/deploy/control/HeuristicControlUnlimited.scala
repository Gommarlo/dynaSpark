/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.control

import scala.collection.mutable

import org.apache.spark.SparkConf

/*
  * Created by Simone Ripamonti on 23/05/2017.
  */
class HeuristicControlUnlimited(conf: SparkConf) extends HeuristicControl(conf) {


  override def computeCores(coresToBeAllocated: Double,
                            executorIndex: Int,
                            stageId: Int,
                            last: Boolean): (Double, Double, Double) = {
    // compute core to start
    val coreForExecutors = computeCoreForExecutors(coresToBeAllocated, stageId, last)

    val coreToStart = coreForExecutors(executorIndex)

    // compute max core
    val coreMax = coreForVM

    // return result
    (coreMin, coreMax, coreToStart)
  }

  override def computeCoreForExecutors(coresToBeAllocated: Double,
                                       stageId: Int, last: Boolean): mutable.IndexedSeq[Double] = {

    numExecutor = math.ceil(coresToBeAllocated.toDouble / coreForVM.toDouble).toInt

    var coresToStart = 0
    if (coresToBeAllocated == coreForVM*numMaxExecutor) {
      coresToStart = math.ceil(coresToBeAllocated.toDouble).toInt
    }
    else {
      coresToStart = math.ceil(coresToBeAllocated.toDouble / OVERSCALE).toInt
    }
    numExecutor = numMaxExecutor
    val immutable = (1 to numMaxExecutor).map { x =>
      math.ceil((coresToStart / numExecutor.toDouble) / CQ) * CQ
    }
    mutable.IndexedSeq(immutable: _*)

  }

  override def computeCoreStage(deadlineStage: Long = 0L, numRecord: Long = 0L,
                                stageId: Int = 0, firstStage : Boolean = false,
                                lastStage: Boolean = false): Double = {

    // if requested cores are greater than the available ones (coreForVM * numMaxExecutor),
    // we fix the value to the available ones
    val requestedCores = super.computeCoreStage(deadlineStage, numRecord, stageId, firstStage, lastStage)
    if (requestedCores > coreForVM * numMaxExecutor) {
      coreForVM * numMaxExecutor
    } else {
      requestedCores
    }

  }
}

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

import spray.json.JsValue

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging



/*
  * Created by Simone Ripamonti on 13/05/2017.
  */
class HeuristicFixed(conf: SparkConf) extends HeuristicBase(conf) with Logging {

  logInfo("USING FIXED CORE/DEADLINE ALLOCATION")

  val stagesToFix: List[Int] = conf.get("spark.control.stage").replace("[", "")
    .replace("]", "").split(',').toList.map(_.trim).map(_.toInt)
  val stageCores: List[Double] = conf.get("spark.control.stagecores").replace("[", "")
    .replace("]", "").split(',').toList.map(_.trim).map(_.toDouble)
  val stageDeadlines: List[Long] = conf.get("spark.control.stagedeadlines").replace("[", "")
    .replace("]", "").split(',').toList.map(_.trim).map(_.toLong)
  val stageToCoresConf = (stagesToFix zip stageCores).toMap
  val stageToDeadlinesConf = (stagesToFix zip stageDeadlines).toMap


  override def computeCores(coresToBeAllocated: Double,
                            executorIndex: Int,
                            stageId: Int,
                            last: Boolean): (Double, Double, Double) = {
    val core = stageToCoresConf(stageId)
    (core, core, core)
  }

  override def computeCoreForExecutors(coresToBeAllocated: Double,
                                       stageId: Int, last: Boolean): mutable.IndexedSeq[Double] = {
    val immutable = (1 to numMaxExecutor).map { x => stageToCoresConf(stageId)}
    mutable.IndexedSeq(immutable: _*)
  }

  override def computeDeadlineStage(startTime: Long,
                                    appDeadlineJobMilliseconds: Long,
                                    totalStageRemaining: Long,
                                    totalDurationRemaining: Long,
                                    stageDuration: Long,
                                    stageId: Int,
                                    firstStage: Boolean = false): Long = {
    stageToDeadlinesConf (stageId)
  }

  override def computeCoreStage(deadlineStage: Long = 0L, numRecord: Long = 0L,
                                stageId : Int, firstStage : Boolean = false,
                                lastStage: Boolean = false): Double = {
    stageToCoresConf(stageId)
  }

  override def computeDeadlineStageWeightGiven(startTime: Long, appDeadlineJobMilliseconds: Long,
                                               weight: Double, stageId: Int,
                                               firstStage: Boolean): Long = {
    stageToDeadlinesConf (stageId)
  }

  override def checkDeadline(appJson: JsValue): Boolean = true
}

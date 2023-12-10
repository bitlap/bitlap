/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.core.sql

import java.io.Serializable

import scala.collection.mutable.ListBuffer

/** Prune time filter to push down
 */
type TimeFilterFun = Long => Boolean

case class PruneTimeFilterExpr(name: String, func: TimeFilterFun, expr: String)

class PruneTimeFilter extends Serializable {

  private val conditions = ListBuffer[PruneTimeFilterExpr]()

  def add(name: String, func: TimeFilterFun, expr: String): PruneTimeFilter = {
    this.conditions += PruneTimeFilterExpr(name, func, expr)
    this
  }

  /** merge conditions into one
   */
  def mergeCondition(): TimeFilterFun = { i =>
    {
      this.conditions.map(_.func).forall(_(i))
    }
  }

  override def toString: String = {
    this.conditions.map(_.expr).mkString("[", ",", "]")
  }
}

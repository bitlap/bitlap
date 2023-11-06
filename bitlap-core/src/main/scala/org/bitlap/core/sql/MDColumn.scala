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

import scala.collection.mutable

import org.bitlap.common.extension._
import org.bitlap.common.utils.PreConditions

import org.apache.calcite.sql.`fun`.SqlCountAggFunction
import org.apache.calcite.sql.`fun`.SqlSumAggFunction
import org.apache.calcite.sql.SqlAggFunction

/** Analyse metric & dimension model from sql select node
 */
final case class MDColumn(name: String, `type`: ColumnType) {

  var project: Boolean = false
  var filter: Boolean  = false

  /** if column is pure with no function call
   */
  var pure: Boolean = false

  /** left: agg function right: function quantifier, for example distinct and etc.
   */
  val aggregates: mutable.Set[(SqlAggFunction, String)] = scala.collection.mutable.Set[(SqlAggFunction, String)]()

  def checkType(targetType: ColumnType): MDColumn = {
    PreConditions.checkExpression(
      `type` == targetType,
      "",
      s"Unable to judge Column $name target type: $targetType should equals to current type: ${`type`}"
    )
    this
  }

  def withFilter(f: Boolean): this.type                      = this.also { it => it.filter = f }
  def withProject(p: Boolean): this.type                     = this.also { it => it.project = p }
  def withPure(p: Boolean): this.type                        = this.also { it => it.pure = p }
  def withAgg(agg: (SqlAggFunction, String)*): this.type     = this.also { it => it.aggregates.addAll(agg) }
  def withAgg(agg: Set[(SqlAggFunction, String)]): this.type = this.also { it => it.aggregates.addAll(agg) }

  def isDistinct: Boolean = {
    if (this.aggregates.isEmpty) {
      return false
    }
    this.aggregates.filter { it => it._1.isInstanceOf[SqlCountAggFunction] }.exists { it =>
      it._2.toLowerCase() == "distinct"
    }
  }

  def isSum: Boolean = {
    if (this.aggregates.isEmpty) {
      return false
    }
    this.aggregates.exists { it => it._1.isInstanceOf[SqlSumAggFunction] }
  }

  override def toString: String = {
    s"MDColumn(name='$name', type=${`type`}, project=$project, filter=$filter, pure=$pure, aggregates=$aggregates)"
  }
}

sealed trait ColumnType
object MetricCol    extends ColumnType
object DimensionCol extends ColumnType

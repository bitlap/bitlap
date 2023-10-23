/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql

import org.bitlap.common.utils.PreConditions
import org.bitlap.core.extension._

import org.apache.calcite.sql.`fun`.SqlCountAggFunction
import org.apache.calcite.sql.`fun`.SqlSumAggFunction
import org.apache.calcite.sql.SqlAggFunction

/** Analyse metric & dimension model from sql select node
 */
case class MDColumn(val name: String, val `type`: ColumnType) {

  var project: Boolean = false
  var filter: Boolean  = false

  /** if column is pure with no function call
   */
  var pure: Boolean = false

  /** left: agg function right: function quantifier, for example distinct and etc.
   */
  val aggregates = scala.collection.mutable.Set[(SqlAggFunction, String)]()

  def checkType(targetType: ColumnType): MDColumn = {
    PreConditions.checkExpression(
      `type` == targetType,
      "",
      s"Unable to judge Column $name target type: $targetType should equals to current type: ${`type`}"
    )
    return this
  }

  def withFilter(f: Boolean)                      = this.also { it => it.filter = f }
  def withProject(p: Boolean)                     = this.also { it => it.project = p }
  def withPure(p: Boolean)                        = this.also { it => it.pure = p }
  def withAgg(agg: (SqlAggFunction, String)*)     = this.also { it => it.aggregates.addAll(agg) }
  def withAgg(agg: Set[(SqlAggFunction, String)]) = this.also { it => it.aggregates.addAll(agg) }

  def isDistinct(): Boolean = {
    if (this.aggregates.isEmpty) {
      return false
    }
    return this.aggregates.filter { it => it._1.isInstanceOf[SqlCountAggFunction] }.exists { it =>
      it._2.toLowerCase() == "distinct"
    }
  }

  def isSum(): Boolean = {
    if (this.aggregates.isEmpty) {
      return false
    }
    return this.aggregates.exists { it => it._1.isInstanceOf[SqlSumAggFunction] }
  }

  override def toString(): String = {
    return s"MDColumn(name='$name', type=${`type`}, project=$project, filter=$filter, pure=$pure, aggregates=$aggregates)"
  }
}

sealed trait ColumnType
object MetricCol    extends ColumnType
object DimensionCol extends ColumnType

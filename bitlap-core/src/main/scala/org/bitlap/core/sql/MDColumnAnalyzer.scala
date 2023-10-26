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
import scala.jdk.CollectionConverters._

import org.bitlap.core.catalog.metadata.Table
import org.bitlap.core.mdm.format.DataType
import org.bitlap.core.mdm.format.DataTypeBBM
import org.bitlap.core.mdm.format.DataTypeCBM
import org.bitlap.core.mdm.format.DataTypeRBM
import org.bitlap.core.mdm.format.DataTypeRowValueMeta

import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.sql.SqlAsOperator
import org.apache.calcite.sql.SqlBasicCall
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlNodeList
import org.apache.calcite.sql.SqlSelect

/** analyzer for [MDColumn] from [SqlSelect]
 */
class MDColumnAnalyzer(val table: Table, val select: SqlSelect) {

  private lazy val mdColumnMap: Map[String, MDColumn] = this.analyseMDColumns(select)
  private lazy val mdColumnNames: List[String]        = this.mdColumnMap.keys.toList.sorted
  private lazy val mdColumns: List[MDColumn]          = this.mdColumnMap.values.toList.sortBy(_.name)

  /** get metric & dimension columns from sql node
   */
  private def analyseMDColumns(node: SqlSelect): Map[String, MDColumn] = {
    val cols = mutable.Map[String, MDColumn]()
    // select
    node.getSelectList.asScala.foreach { it =>
      this.getColumnName(it).foreach { c =>
        if (cols.contains(c.name)) {
          cols(c.name)
            .checkType(c.`type`)
            .withProject(true)
            .withAgg(c.aggregates.toSet)
        } else {
          cols(c.name) = c.withProject(true)
        }
      }
    }
    // from: ignore
    // where: treated as dimensions currently
    if (node.getWhere != null) {
      this.getColumnName(node.getWhere).foreach { c =>
        if (cols.contains(c.name)) {
          cols(c.name)
            .checkType(c.`type`)
            .withFilter(true)
        } else {
          cols(c.name) = c.withFilter(true)
        }
      }
    }
    // group by: treated as dimensions
    val group = node.getGroup
    if (group != null) {
      group.asScala.foreach { it =>
        this.getColumnName(it).foreach { c =>
          if (cols.contains(c.name)) {
            cols(c.name).checkType(c.`type`)
          } else {
            cols(c.name) = c
          }
        }
        it match {
          case g: SqlIdentifier =>
            cols(g.names.asScala.last)
              .withPure(true)
          case _ =>
        }
      }
    }
    cols.toMap
  }

  private def getColumnName(_node: SqlNode): List[MDColumn] = {
    _node match {
      case node: SqlIdentifier =>
        if (node.isStar) {
          throw IllegalArgumentException(s"It is forbidden to query * from ${table.database}.${table.name}")
        }
        List(MDColumn(node.names.asScala.last, DimensionCol))
      case node: SqlBasicCall =>
        node.getOperator match {
          case _: SqlAsOperator =>
            getColumnName(node.getOperandList.asScala.head)
          case _: SqlAggFunction =>
            node.getOperandList.asScala
              .flatMap(getColumnName)
              .map { it =>
                val operator = node.getOperator.asInstanceOf[SqlAggFunction]
                MDColumn(it.name, MetricCol)
                  .withAgg(operator -> Option(node.getFunctionQuantifier).map(_.toValue).getOrElse(""))
              }
              .toList
          case _ =>
            node.getOperandList.asScala.flatMap(getColumnName).toList
        }
      case node: SqlNodeList =>
        node.getList.asScala.filter(_ != null).flatMap(getColumnName).toList
      case node: SqlLiteral => List.empty[MDColumn]
      case node             => throw IllegalArgumentException(s"Illegal node: $node")
    }
  }

  def getMetricColNames: List[String]    = this.mdColumns.filter(_.`type` == MetricCol).map(_.name).distinct
  def getDimensionColNames: List[String] = this.mdColumns.filter(_.`type` == DimensionCol).map(_.name).distinct
  def getFilterColNames: List[String]    = this.mdColumns.filter(_.filter).map(_.name).distinct
  def getDimensionColNamesWithoutTime: List[String] = this.getDimensionColNames.filter(_ != Keyword.TIME)

  /** check if one metric should materialize
   */
  def shouldMaterialize(metricName: String): Boolean = {
    val metric     = this.mdColumnMap(metricName)
    val dimensions = this.getDimensionColNames

    // check whether the metric is Cartesian Product
    if (this.shouldCartesian()) {
      return true
    }

    // no dimensions includes time
    // 1) distinct aggregation: true
    // 2) sum, count aggregation: false
    if (dimensions.isEmpty) {
      if (metric.isDistinct) {
        return true
      }
      return false
    }

    // has no pure dimensions includes time
    // 1) dimension is complex expression
    // 2) dimension is not in group by
    val hasNoPureDims = this.mdColumns.exists { it => it.`type` == DimensionCol && !it.pure }
    if (hasNoPureDims && metric.isDistinct) {
      return true
    }
    false
  }

  /** check whether the metric is Cartesian Product
   */
  def shouldCartesian(): Boolean = {
    // there are more than or equal to 2 dimensions except time
    val noTimeDims = this.getDimensionColNamesWithoutTime
    if (noTimeDims.size >= 2) {
      return true
    }
    false
  }

  /** get metric materialize type
   */
  def materializeType(metricName: String, cartesianBase: Boolean): DataType = {
    val metric      = this.mdColumnMap(metricName)
    val materialize = this.shouldMaterialize(metricName)

    // not materialize
    if (!materialize) {
      return DataTypeRowValueMeta(metricName, -1)
    }

    //  materialize & Cartesian Product
    if (this.shouldCartesian()) {
      if (metric.isSum) {
        return if (cartesianBase) DataTypeCBM(metricName, -1) else DataTypeBBM(metricName, -1)
      }
      if (metric.isDistinct) {
        return DataTypeRBM(metricName, -1)
      }
    }

    // materialize & not Cartesian Product
    if (metric.isSum && metric.isDistinct) {
      return DataTypeCBM(metricName, -1)
    }
    if (metric.isDistinct) {
      return DataTypeRBM(metricName, -1)
    }
    throw IllegalArgumentException(s"Invalid input metric type, metric is $metric")
  }
}

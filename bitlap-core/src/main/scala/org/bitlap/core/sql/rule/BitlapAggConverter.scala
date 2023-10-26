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
package org.bitlap.core.sql.rule

import scala.jdk.CollectionConverters._

import org.bitlap.core.sql.rel.BitlapAggregate
import org.bitlap.core.sql.udf.FunctionRegistry
import org.bitlap.core.sql.udf.UDFNames

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.sql.`fun`.SqlAbstractGroupFunction
import org.apache.calcite.sql.`fun`.SqlCountAggFunction
import org.apache.calcite.sql.`fun`.SqlMinMaxAggFunction
import org.apache.calcite.sql.`fun`.SqlSumAggFunction
import org.apache.calcite.sql.`fun`.SqlSumEmptyIsZeroAggFunction
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.SqlAggFunction

/** convert sum, count, count_distinct to internal agg type.
 */
class BitlapAggConverter extends AbsRelRule(classOf[BitlapAggregate], "BitlapAggConverter") {

  import BitlapAggConverter._

  override def convert0(_rel: RelNode, call: RelOptRuleCall): RelNode = {
    val rel = _rel.asInstanceOf[BitlapAggregate]
    // 1. if it has no table scan, no need to convert
    // 2. if current is not first BitlapAggregate deeply, maybe sub query, no need to convert
    if (!this.hasTableScanNode(rel) || this.hasSecondAggregate(rel)) {
      return rel
    }
    val typeFactory = call.builder().getTypeFactory
    // check need converts
    val need = rel.getAggCallList.asScala.exists { it => NEED_CONVERTS.contains(it.getAggregation.getClass) }
    if (!need) {
      return rel
    }

    // convert aggregate functions
    val aggCalls = rel.getAggCallList.asScala.map { it =>
      val aggFunc  = it.getAggregation
      var `type`   = it.getType
      var distinct = it.isDistinct
      val func = aggFunc match {
        case _: SqlSumAggFunction | _: SqlSumEmptyIsZeroAggFunction =>
          `type` = typeFactory.createSqlType(SqlTypeName.DOUBLE)
          FunctionRegistry.getFunction(UDFNames.bm_sum_aggr).asInstanceOf[SqlAggFunction]
        case _: SqlCountAggFunction =>
          if (it.isDistinct) {
            `type` = typeFactory.createSqlType(SqlTypeName.BIGINT)
            distinct = false
            FunctionRegistry.getFunction(UDFNames.bm_count_distinct).asInstanceOf[SqlAggFunction]
          } else {
            `type` = typeFactory.createSqlType(SqlTypeName.BIGINT)
            FunctionRegistry.getFunction(UDFNames.bm_count_aggr).asInstanceOf[SqlAggFunction]
          }
        case _: SqlMinMaxAggFunction | _: SqlAbstractGroupFunction =>
          aggFunc
        case _ =>
          if (FunctionRegistry.contains(aggFunc.getName)) {
            aggFunc
          } else {
            throw IllegalArgumentException(s"${aggFunc.getName} aggregate function is not supported.")
          }
      }
      AggregateCall.create(
        func,
        distinct,
        it.isApproximate,
        it.ignoreNulls(),
        it.getArgList,
        it.filterArg,
        it.distinctKeys,
        it.collation,
        `type`,
        it.name
      )
    }
    rel.withAggCalls(aggCalls.asJava)
  }

  private def hasSecondAggregate(rel: RelNode, inputs: Boolean = false): Boolean = {
    if (rel.isInstanceOf[BitlapAggregate] && inputs) {
      return true
    }
    rel.getInputs.asScala.map(_.clean()).filter(_ != null).exists { it => this.hasSecondAggregate(it, true) }
  }
}

object BitlapAggConverter {

  // TODO (see AggregateReduceFunctionsRule, support other aggregate functions)
  private val NEED_CONVERTS = List(
    classOf[SqlSumAggFunction],
    classOf[SqlSumEmptyIsZeroAggFunction],
    classOf[SqlCountAggFunction]
    // SqlAvgAggFunction::class.java,
  )
}

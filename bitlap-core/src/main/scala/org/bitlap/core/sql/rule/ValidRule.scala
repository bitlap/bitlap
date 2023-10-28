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

import org.bitlap.core.sql.Keyword
import org.bitlap.core.sql.rel.BitlapNode
import org.bitlap.core.sql.rel.BitlapTableScan

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.RelNode

class ValidRule extends AbsRelRule(classOf[BitlapNode], "ValidRule") {

  override def convert0(_rel: RelNode, call: RelOptRuleCall): RelNode = {
    _rel match {
      case rel: BitlapTableScan =>
        val table   = rel.getOTable
        val dimCols = table.analyzer.filterColNames
        // check time filter
        if (!dimCols.contains(Keyword.TIME)) {
          throw IllegalArgumentException(s"Query must contain ${Keyword.TIME} filter")
        }
        // check metrics
        val metricCols = table.analyzer.metricColNames
        if (metricCols.isEmpty) {
          throw IllegalArgumentException(
            s"Query must contain at least one aggregation metric, aggregation column must be specified"
          )
        }
      case _ =>
    }
    _rel
  }
}

/**
 * Copyright (C) 2023 bitlap.org .
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
      case rel: BitlapTableScan => {
        val table   = rel.getOTable()
        val dimCols = table.analyzer.getFilterColNames()
        // check time filter
        if (!dimCols.contains(Keyword.TIME)) {
          throw IllegalArgumentException(s"Query must contain ${Keyword.TIME} filter")
        }
        // check metrics
        val metricCols = table.analyzer.getMetricColNames()
        if (metricCols.isEmpty) {
          throw IllegalArgumentException(
            s"Query must contain at least one aggregation metric, aggregation column must be specified"
          )
        }
      }
    }
    return _rel
  }
}

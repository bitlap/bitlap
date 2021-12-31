package org.bitlap.core.sql.rule

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.RelNode
import org.bitlap.core.sql.Keyword
import org.bitlap.core.sql.rel.BitlapNode
import org.bitlap.core.sql.rel.BitlapTableScan

class ValidRule : AbsRelRule(BitlapNode::class.java, "ValidRule") {

    override fun convert0(rel: RelNode, call: RelOptRuleCall): RelNode {
        when (rel) {
            is BitlapTableScan -> {
                val table = rel.getOTable()
                val dimCols = table.analyzer.getFilterColNames()
                // check time filter
                if (!dimCols.contains(Keyword.TIME)) {
                    throw IllegalArgumentException("Query must contain ${Keyword.TIME} filter")
                }
                // check metrics
                val metricCols = table.analyzer.getMetricColNames()
                if (metricCols.isEmpty()) {
                    throw IllegalArgumentException("Query must contain at least one aggregation metric, aggregation column must be specified")
                }
            }
        }
        return rel
    }
}

/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.rule.enumerable

import org.apache.calcite.adapter.enumerable.EnumerableAggregate
import org.apache.calcite.adapter.enumerable.EnumerableConvention
import org.apache.calcite.adapter.enumerable.EnumerableRules
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.InvalidRelException
import org.apache.calcite.rel.RelNode
import org.bitlap.core.sql.rel.BitlapAggregate
import org.bitlap.core.sql.rule.AbsRelRule

/**
 * Convert BitlapAggregate to enumerable rule.
 *
 * @see [org.apache.calcite.adapter.enumerable.EnumerableAggregateRule]
 * @see [EnumerableRules.ENUMERABLE_AGGREGATE_RULE]
 */
class BitlapEnumerableAggregateRule : AbsRelRule(BitlapAggregate::class.java, "BitlapEnumerableAggregateRule") {

    override fun convert0(rel: RelNode, call: RelOptRuleCall): RelNode? {
        val agg = rel as BitlapAggregate
        val traitSet = rel.getCluster().traitSet().replace(EnumerableConvention.INSTANCE)
        return try {
            EnumerableAggregate(
                rel.getCluster(),
                traitSet,
                convert(agg.input, traitSet),
                agg.groupSet,
                agg.getGroupSets(),
                agg.aggCallList
            )
        } catch (e: InvalidRelException) {
            log.debug(e.toString())
            null
        }
    }
}

/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.sql.rule.enumerable

import org.apache.calcite.adapter.enumerable.EnumerableConvention
import org.apache.calcite.adapter.enumerable.EnumerableRules
import org.apache.calcite.adapter.enumerable.EnumerableSortedAggregate
import org.apache.calcite.plan.Convention
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.RelCollations
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.util.ImmutableIntList
import org.bitlap.core.sql.rel.BitlapAggregate
import org.bitlap.core.sql.rule.AbsRelRule

/**
 * Convert BitlapAggregate to enumerable rule.
 *
 * @see [org.apache.calcite.adapter.enumerable.EnumerableSortedAggregateRule]
 * @see [EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE]
 */
class BitlapEnumerableAggregateSortedRule : AbsRelRule(CONFIG) {

    companion object {
        private val CONFIG = Config.INSTANCE
            .withConversion(
                BitlapAggregate::class.java, Convention.NONE,
                EnumerableConvention.INSTANCE, "BitlapEnumerableAggregateSortedRule"
            )
    }

    override fun convert0(rel: RelNode, call: RelOptRuleCall): RelNode? {
        val agg = rel as Aggregate
        if (!Aggregate.isSimple(agg)) {
            return null
        }
        val inputTraits = rel.getCluster()
            .traitSet().replace(EnumerableConvention.INSTANCE)
            .replace(
                RelCollations.of(ImmutableIntList.copyOf(agg.groupSet.asList()))
            )
        val selfTraits = inputTraits.replace(
            RelCollations.of(ImmutableIntList.identity(agg.groupSet.cardinality()))
        )
        return EnumerableSortedAggregate(
            rel.getCluster(),
            selfTraits,
            convert(agg.input, inputTraits),
            agg.groupSet,
            agg.getGroupSets(),
            agg.aggCallList,
        )
    }
}

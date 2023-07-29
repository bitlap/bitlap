/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.rule.enumerable

import org.apache.calcite.adapter.enumerable.EnumerableConvention
import org.apache.calcite.adapter.enumerable.EnumerableFilter
import org.apache.calcite.adapter.enumerable.EnumerableRules
import org.apache.calcite.plan.Convention
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Filter
import org.bitlap.core.sql.rel.BitlapFilter
import org.bitlap.core.sql.rule.AbsRelRule

/**
 * Convert BitlapFilter to enumerable rule.
 *
 * @see [org.apache.calcite.adapter.enumerable.EnumerableFilterRule]
 * @see [EnumerableRules.ENUMERABLE_FILTER_RULE]
 */
class BitlapEnumerableFilterRule : AbsRelRule(CONFIG) {

    companion object {
        private val CONFIG = Config.INSTANCE.withConversion(
            BitlapFilter::class.java,
            { !it.containsOver() },
            Convention.NONE, EnumerableConvention.INSTANCE,
            "BitlapEnumerableFilterRule"
        )
    }

    override fun convert0(rel: RelNode, call: RelOptRuleCall): RelNode? {
        val filter = rel as Filter
        return EnumerableFilter(
            rel.getCluster(),
            rel.getTraitSet().replace(EnumerableConvention.INSTANCE),
            convert(
                filter.input,
                filter.input.traitSet.replace(EnumerableConvention.INSTANCE)
            ),
            filter.condition
        )
    }
}

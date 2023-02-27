/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.sql.rule.enumerable

import org.apache.calcite.adapter.enumerable.EnumerableConvention
import org.apache.calcite.adapter.enumerable.EnumerableRules
import org.apache.calcite.adapter.enumerable.EnumerableUnion
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Union
import org.apache.calcite.util.Util
import org.bitlap.core.sql.rel.BitlapUnion
import org.bitlap.core.sql.rule.AbsRelRule

/**
 * Convert BitlapUnion to enumerable rule.
 *
 * @see [org.apache.calcite.adapter.enumerable.EnumerableUnionRule]
 * @see [EnumerableRules.ENUMERABLE_UNION_RULE]
 */
class BitlapEnumerableUnionRule : AbsRelRule(BitlapUnion::class.java, "BitlapEnumerableUnionRule") {

    override fun convert0(rel: RelNode, call: RelOptRuleCall): RelNode {
        val union = rel as Union
        val traitSet = rel.getCluster().traitSet().replace(EnumerableConvention.INSTANCE)
        val newInputs = Util.transform(union.inputs) { convert(it, traitSet) }
        return EnumerableUnion(
            rel.getCluster(),
            traitSet,
            newInputs,
            union.all,
        )
    }
}

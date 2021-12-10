package org.bitlap.core.sql.rule

import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.rules.CoreRules
import org.bitlap.core.sql.rel.BitlapNode
import org.bitlap.core.sql.rule.enumerable.BitlapEnumerableAggregateRule
import org.bitlap.core.sql.rule.enumerable.BitlapEnumerableAggregateSortedRule
import org.bitlap.core.sql.rule.enumerable.BitlapEnumerableFilterRule
import org.bitlap.core.sql.rule.enumerable.BitlapEnumerableProjectRule
import org.bitlap.core.sql.rule.enumerable.BitlapEnumerableUnionMergeRule
import org.bitlap.core.sql.rule.enumerable.BitlapEnumerableUnionRule

/**
 * Entrance functions for sql rules.
 */

/**
 * @see [org.apache.calcite.rel.rules.CoreRules]
 */
val RULES = listOf(
    // rules to transform calcite logical node plan
    listOf(
        CoreRules.UNION_MERGE,
        BitlapAggConverter(),
    ),
    // rules to transform bitlap node plan
    listOf(
        BitlapRelConverter(),
        BitlapFilterTableScanRule(),
        ValidRule(),
        BitlapTableConverter(),
    )
)

/**
 * @see [org.apache.calcite.tools.Programs.RULE_SET]
 * @see [org.apache.calcite.adapter.enumerable.EnumerableRules.ENUMERABLE_RULES]
 */
val ENUMERABLE_RULES = listOf(
    BitlapEnumerableAggregateRule(),
    BitlapEnumerableAggregateSortedRule(),
    BitlapEnumerableProjectRule(),
    BitlapEnumerableFilterRule(),
    BitlapEnumerableUnionRule(),
    BitlapEnumerableUnionMergeRule(),
)

/**
 * clean HepRelVertex wrapper
 */
fun RelNode?.clean(): RelNode? {
    return when (this) {
        is HepRelVertex -> this.currentRel
        else -> this
    }
}

/**
 * inject parent node
 */
fun RelNode.injectParent(parent: (RelNode) -> RelNode): RelNode {
    val p = parent(this)
    when {
        this is BitlapNode ->
            this.parent = p
        this is HepRelVertex && this.currentRel is BitlapNode ->
            (this.currentRel as BitlapNode).parent = p
    }
    return p
}

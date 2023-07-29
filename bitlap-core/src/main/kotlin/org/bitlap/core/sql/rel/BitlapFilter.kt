/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.rel

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.core.CorrelationId
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.rex.RexNode
import java.util.Objects

/**
 * Filter logical plan, see [org.apache.calcite.rel.logical.LogicalFilter]
 */
class BitlapFilter(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    child: RelNode,
    condition: RexNode,
    private val variablesSet: Set<CorrelationId>,
    override var parent: RelNode? = null,
) : Filter(cluster, traitSet, child, condition), BitlapNode {

    override fun copy(traitSet: RelTraitSet, input: RelNode, condition: RexNode): Filter {
        return BitlapFilter(cluster, traitSet, input, condition, variablesSet, parent)
    }

    override fun explainTerms(pw: RelWriter?): RelWriter? {
        return super.explainTerms(pw)
            .itemIf("variablesSet", variablesSet, variablesSet.isNotEmpty())
    }

    override fun deepEquals(obj: Any?): Boolean {
        return (
            deepEquals0(obj) &&
                variablesSet == (obj as? BitlapFilter)?.variablesSet
            )
    }

    override fun deepHashCode(): Int {
        return Objects.hash(deepHashCode0(), variablesSet)
    }

    override fun getVariablesSet(): Set<CorrelationId> {
        return variablesSet
    }
}

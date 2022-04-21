/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.sql.rel

import com.google.common.collect.ImmutableSet
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.core.CorrelationId
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.rel.logical.LogicalFilter
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
    private val variablesSet: ImmutableSet<CorrelationId> = ImmutableSet.of(),
    override var parent: RelNode? = null,
) : Filter(cluster, traitSet, child, condition), BitlapNode {

    constructor(
        cluster: RelOptCluster,
        traitSet: RelTraitSet,
        child: RelNode,
        condition: RexNode,
        variablesSet: Set<CorrelationId>,
        parent: RelNode? = null,
    ) : this(
        cluster,
        traitSet,
        child,
        condition,
        if (variablesSet is ImmutableSet) variablesSet else ImmutableSet.copyOf(variablesSet),
        parent,
    )

    override fun copy(traitSet: RelTraitSet, input: RelNode, condition: RexNode): Filter {
        return BitlapFilter(cluster, traitSet, input, condition, variablesSet, parent)
    }

    override fun explainTerms(pw: RelWriter?): RelWriter? {
        return super.explainTerms(pw)
            .itemIf("variablesSet", variablesSet, !variablesSet.isEmpty())
    }

    override fun deepEquals(obj: Any?): Boolean {
        return (
            deepEquals0(obj) &&
                variablesSet == (obj as LogicalFilter?)!!.variablesSet
            )
    }

    override fun deepHashCode(): Int {
        return Objects.hash(deepHashCode0(), variablesSet)
    }
}

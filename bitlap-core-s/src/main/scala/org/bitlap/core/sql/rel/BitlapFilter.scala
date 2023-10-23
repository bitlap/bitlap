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

import java.util.Set as JSet

/**
 * Filter logical plan, see [org.apache.calcite.rel.logical.LogicalFilter]
 */
class BitlapFilter(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    child: RelNode,
    condition: RexNode,
    private val variablesSet: JSet[CorrelationId],
    var parent: RelNode = null
) extends Filter(cluster, traitSet, child, condition) with BitlapNode {

    override def copy(traitSet: RelTraitSet, input: RelNode, condition: RexNode): Filter = {
        return BitlapFilter(cluster, traitSet, input, condition, variablesSet, parent)
    }

    override def explainTerms(pw: RelWriter): RelWriter = {
        return super.explainTerms(pw)
            .itemIf("variablesSet", variablesSet, !variablesSet.isEmpty)
    }

    override def deepEquals(obj: Any): Boolean = {
        deepEquals0(obj) &&
                variablesSet == obj.asInstanceOf[BitlapFilter].variablesSet
    }

    override def deepHashCode(): Int = {
        return Objects.hash(deepHashCode0(), variablesSet)
    }

    override def getVariablesSet: JSet[CorrelationId] = {
        return variablesSet
    }
}

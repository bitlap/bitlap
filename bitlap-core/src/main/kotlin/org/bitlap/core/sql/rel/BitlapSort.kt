/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.sql.rel

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rex.RexNode

/**
 * Sort logical plan, see [org.apache.calcite.rel.logical.LogicalSort]
 */
class BitlapSort(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    collation: RelCollation,
    offset: RexNode?,
    fetch: RexNode?,
    override var parent: RelNode? = null,
) : Sort(cluster, traitSet, input, collation, offset, fetch), BitlapNode {

    override fun copy(traitSet: RelTraitSet, newInput: RelNode, newCollation: RelCollation, offset: RexNode?, fetch: RexNode?): Sort {
        return BitlapSort(cluster, traitSet, newInput, newCollation, offset, fetch, parent)
    }
}

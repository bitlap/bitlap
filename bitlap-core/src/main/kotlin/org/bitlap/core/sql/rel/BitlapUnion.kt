/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.rel

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.SetOp
import org.apache.calcite.rel.core.Union

/**
 * Union logical plan, see [org.apache.calcite.rel.logical.LogicalUnion]
 */
class BitlapUnion(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    inputs: List<RelNode>,
    all: Boolean,
    override var parent: RelNode? = null
) : Union(cluster, traits, inputs, all), BitlapNode {

    override fun copy(traitSet: RelTraitSet, inputs: MutableList<RelNode>, all: Boolean): SetOp {
        return BitlapUnion(this.cluster, traitSet, inputs, all)
    }
}

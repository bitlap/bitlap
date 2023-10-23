/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.rel

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.SetOp
import org.apache.calcite.rel.core.Union

import java.util.List as JList

/**
 * Union logical plan, see [org.apache.calcite.rel.logical.LogicalUnion]
 */
class BitlapUnion(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    inputs: JList[RelNode],
    all: Boolean,
    var parent: RelNode = null
) extends Union(cluster, traits, inputs, all) with BitlapNode {

    override def copy(traitSet: RelTraitSet, inputs: JList[RelNode], all: Boolean): SetOp = {
        return BitlapUnion(getCluster, traitSet, inputs, all)
    }
}

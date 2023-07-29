/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.rel

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.util.ImmutableBitSet

/**
 * Aggregate logical plan, see [org.apache.calcite.rel.logical.LogicalAggregate]
 */
class BitlapAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    hints: List<RelHint>,
    input: RelNode,
    groupSet: ImmutableBitSet,
    groupSets: List<ImmutableBitSet>?,
    aggCalls: List<AggregateCall>,
    override var parent: RelNode? = null
) : Aggregate(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls), BitlapNode {

    override fun copy(
        traitSet: RelTraitSet,
        input: RelNode,
        groupSet: ImmutableBitSet,
        groupSets: MutableList<ImmutableBitSet>?,
        aggCalls: MutableList<AggregateCall>
    ): Aggregate {
        return BitlapAggregate(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls, parent)
    }

    override fun withHints(hintList: MutableList<RelHint>): RelNode {
        return BitlapAggregate(cluster, traitSet, hintList, input, groupSet, groupSets, aggCalls, parent)
    }

    fun withAggCalls(aggCalls: List<AggregateCall>): BitlapAggregate {
        return BitlapAggregate(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls, parent)
    }

    fun copy(input: RelNode, aggCalls: List<AggregateCall>): BitlapAggregate {
        return BitlapAggregate(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls, parent)
    }
}

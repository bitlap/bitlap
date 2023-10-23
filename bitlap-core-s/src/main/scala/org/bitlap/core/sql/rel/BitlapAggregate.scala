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

import java.util.List as JList

/**
 * Aggregate logical plan, see [org.apache.calcite.rel.logical.LogicalAggregate]
 */
class BitlapAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    hints: JList[RelHint],
    input: RelNode,
    groupSet: ImmutableBitSet,
    groupSets: JList[ImmutableBitSet],
    aggCalls: JList[AggregateCall],
    var parent: RelNode = null
) extends Aggregate(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls) with BitlapNode {

    override def copy(
        traitSet: RelTraitSet,
        input: RelNode,
        groupSet: ImmutableBitSet,
        groupSets: JList[ImmutableBitSet],
        aggCalls: JList[AggregateCall]
    ): Aggregate = {
        return BitlapAggregate(getCluster, traitSet, getHints, input, groupSet, groupSets, aggCalls, parent)
    }

    override def withHints(hintList: JList[RelHint]): RelNode = {
        return BitlapAggregate(getCluster, getTraitSet, hintList, getInput, getGroupSet, getGroupSets, getAggCallList, parent)
    }

    def withAggCalls(aggCalls: JList[AggregateCall]): BitlapAggregate = {
        return BitlapAggregate(getCluster, getTraitSet, getHints, getInput, getGroupSet, getGroupSets, aggCalls, parent)
    }

    def copy(input: RelNode, aggCalls: JList[AggregateCall]): BitlapAggregate = {
        return BitlapAggregate(getCluster, getTraitSet, getHints, input, getGroupSet, getGroupSets, aggCalls, parent)
    }
}

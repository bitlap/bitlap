/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.sql.rel

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.logical.LogicalValues
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rex.RexLiteral

class BitlapValues(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    rowType: RelDataType,
    tuples: ImmutableList<ImmutableList<RexLiteral>>,
) : LogicalValues(cluster, traitSet, rowType, tuples)

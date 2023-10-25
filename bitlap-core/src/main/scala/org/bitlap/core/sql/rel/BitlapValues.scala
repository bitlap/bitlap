/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.rel

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalValues
import org.apache.calcite.rex.RexLiteral

import com.google.common.collect.ImmutableList

class BitlapValues(
  cluster: RelOptCluster,
  traitSet: RelTraitSet,
  rowType: RelDataType,
  tuples: ImmutableList[ImmutableList[RexLiteral]],
  var parent: RelNode = null)
    extends LogicalValues(cluster, traitSet, rowType, tuples)
    with BitlapNode

/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.rule.enumerable

import org.bitlap.core.sql.rel.BitlapUnion
import org.bitlap.core.sql.rule.AbsRelRule

import org.apache.calcite.adapter.enumerable.EnumerableConvention
import org.apache.calcite.adapter.enumerable.EnumerableUnion
import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Union
import org.apache.calcite.util.Util

/** Convert BitlapUnion to enumerable rule.
 *
 *  @see
 *    [org.apache.calcite.adapter.enumerable.EnumerableUnionRule]
 *  @see
 *    [EnumerableRules.ENUMERABLE_UNION_RULE]
 */
class BitlapEnumerableUnionRule extends AbsRelRule(classOf[BitlapUnion], "BitlapEnumerableUnionRule") {

  override def convert0(rel: RelNode, call: RelOptRuleCall): RelNode = {
    val union     = rel.asInstanceOf[Union]
    val traitSet  = rel.getCluster.traitSet().replace(EnumerableConvention.INSTANCE)
    val newInputs = Util.transform(union.getInputs, (it: RelNode) => RelOptRule.convert(it, traitSet))
    EnumerableUnion(
      rel.getCluster,
      traitSet,
      newInputs,
      union.all
    )
  }
}

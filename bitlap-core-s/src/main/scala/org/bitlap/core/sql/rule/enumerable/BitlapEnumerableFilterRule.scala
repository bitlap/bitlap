/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.rule.enumerable

import org.bitlap.core.sql.rel.BitlapFilter
import org.bitlap.core.sql.rule.AbsRelRule

import org.apache.calcite.adapter.enumerable.EnumerableConvention
import org.apache.calcite.adapter.enumerable.EnumerableFilter
import org.apache.calcite.adapter.enumerable.EnumerableRules
import org.apache.calcite.plan.Convention
import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.Filter

/** Convert BitlapFilter to enumerable rule.
 *
 *  @see
 *    [org.apache.calcite.adapter.enumerable.EnumerableFilterRule]
 *  @see
 *    [EnumerableRules.ENUMERABLE_FILTER_RULE]
 */
class BitlapEnumerableFilterRule extends AbsRelRule(BitlapEnumerableFilterRule.CONFIG) {

  override def convert0(rel: RelNode, call: RelOptRuleCall): RelNode = {
    val filter = rel.asInstanceOf[Filter]
    return EnumerableFilter(
      rel.getCluster(),
      rel.getTraitSet().replace(EnumerableConvention.INSTANCE),
      RelOptRule.convert(
        filter.getInput,
        filter.getInput.getTraitSet.replace(EnumerableConvention.INSTANCE)
      ),
      filter.getCondition
    )
  }
}

object BitlapEnumerableFilterRule {

  val CONFIG: ConverterRule.Config = ConverterRule.Config.INSTANCE.withConversion(
    classOf[BitlapFilter],
    (input: BitlapFilter) => !input.containsOver(),
    Convention.NONE,
    EnumerableConvention.INSTANCE,
    "BitlapEnumerableFilterRule"
  )
}

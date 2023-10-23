/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.rule.enumerable

import org.bitlap.core.sql.rel.BitlapAggregate
import org.bitlap.core.sql.rule.AbsRelRule

import org.apache.calcite.adapter.enumerable.EnumerableConvention
import org.apache.calcite.adapter.enumerable.EnumerableRules
import org.apache.calcite.adapter.enumerable.EnumerableSortedAggregate
import org.apache.calcite.plan.Convention
import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.RelCollations
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.util.ImmutableIntList

/** Convert BitlapAggregate to enumerable rule.
 *
 *  @see
 *    [org.apache.calcite.adapter.enumerable.EnumerableSortedAggregateRule]
 *  @see
 *    [EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE]
 */
class BitlapEnumerableAggregateSortedRule extends AbsRelRule(BitlapEnumerableAggregateSortedRule.CONFIG) {

  override def convert0(rel: RelNode, call: RelOptRuleCall): RelNode = {
    val agg = rel.asInstanceOf[Aggregate]
    if (!Aggregate.isSimple(agg)) {
      return null
    }
    val inputTraits = rel
      .getCluster()
      .traitSet()
      .replace(EnumerableConvention.INSTANCE)
      .replace(
        RelCollations.of(ImmutableIntList.copyOf(agg.getGroupSet.asList()))
      )
    val selfTraits = inputTraits.replace(
      RelCollations.of(ImmutableIntList.identity(agg.getGroupSet.cardinality()))
    )
    return EnumerableSortedAggregate(
      rel.getCluster(),
      selfTraits,
      RelOptRule.convert(agg.getInput, inputTraits),
      agg.getGroupSet,
      agg.getGroupSets(),
      agg.getAggCallList
    )
  }
}

object BitlapEnumerableAggregateSortedRule {

  val CONFIG = ConverterRule.Config.INSTANCE
    .withConversion(
      classOf[BitlapAggregate],
      Convention.NONE,
      EnumerableConvention.INSTANCE,
      "BitlapEnumerableAggregateSortedRule"
    )
}

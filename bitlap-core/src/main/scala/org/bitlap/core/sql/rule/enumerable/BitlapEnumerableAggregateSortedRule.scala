/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.core.sql.rule.enumerable

import org.bitlap.core.sql.rel.BitlapAggregate
import org.bitlap.core.sql.rule.AbsRelRule

import org.apache.calcite.adapter.enumerable.{ EnumerableConvention, EnumerableSortedAggregate }
import org.apache.calcite.plan.{ Convention, RelOptRule, RelOptRuleCall }
import org.apache.calcite.rel.{ RelCollations, RelNode }
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
    val inputTraits = rel.getCluster
      .traitSet()
      .replace(EnumerableConvention.INSTANCE)
      .replace(
        RelCollations.of(ImmutableIntList.copyOf(agg.getGroupSet.asList()))
      )
    val selfTraits = inputTraits.replace(
      RelCollations.of(ImmutableIntList.identity(agg.getGroupSet.cardinality()))
    )
    EnumerableSortedAggregate(
      rel.getCluster,
      selfTraits,
      RelOptRule.convert(agg.getInput, inputTraits),
      agg.getGroupSet,
      agg.getGroupSets,
      agg.getAggCallList
    )
  }
}

object BitlapEnumerableAggregateSortedRule {

  val CONFIG: ConverterRule.Config = ConverterRule.Config.INSTANCE
    .withConversion(
      classOf[BitlapAggregate],
      Convention.NONE,
      EnumerableConvention.INSTANCE,
      "BitlapEnumerableAggregateSortedRule"
    )
}

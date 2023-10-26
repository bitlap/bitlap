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

import org.bitlap.core.sql.rel.BitlapFilter
import org.bitlap.core.sql.rule.AbsRelRule

import org.apache.calcite.adapter.enumerable.{ EnumerableConvention, EnumerableFilter }
import org.apache.calcite.plan.{ Convention, RelOptRule, RelOptRuleCall }
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
    EnumerableFilter(
      rel.getCluster,
      rel.getTraitSet.replace(EnumerableConvention.INSTANCE),
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

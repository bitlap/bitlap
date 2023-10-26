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

import org.apache.calcite.adapter.enumerable.{ EnumerableAggregate, EnumerableConvention }
import org.apache.calcite.plan.{ RelOptRule, RelOptRuleCall }
import org.apache.calcite.rel.{ InvalidRelException, RelNode }

/** Convert BitlapAggregate to enumerable rule.
 *
 *  @see
 *    [org.apache.calcite.adapter.enumerable.EnumerableAggregateRule]
 *  @see
 *    [EnumerableRules.ENUMERABLE_AGGREGATE_RULE]
 */
class BitlapEnumerableAggregateRule extends AbsRelRule(classOf[BitlapAggregate], "BitlapEnumerableAggregateRule") {

  override def convert0(rel: RelNode, call: RelOptRuleCall): RelNode = {
    val agg      = rel.asInstanceOf[BitlapAggregate]
    val traitSet = rel.getCluster.traitSet().replace(EnumerableConvention.INSTANCE)
    try {
      EnumerableAggregate(
        rel.getCluster,
        traitSet,
        RelOptRule.convert(agg.getInput, traitSet),
        agg.getGroupSet,
        agg.getGroupSets,
        agg.getAggCallList
      )
    } catch {
      case e: InvalidRelException =>
        log.debug(e.toString)
        null
    }
  }
}

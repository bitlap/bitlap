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

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
package org.bitlap.core.sql.rule

import org.bitlap.core.sql.rel.BitlapNode
import org.bitlap.core.sql.rule.enumerable.BitlapEnumerableAggregateRule
import org.bitlap.core.sql.rule.enumerable.BitlapEnumerableAggregateSortedRule
import org.bitlap.core.sql.rule.enumerable.BitlapEnumerableFilterRule
import org.bitlap.core.sql.rule.enumerable.BitlapEnumerableProjectRule
import org.bitlap.core.sql.rule.enumerable.BitlapEnumerableUnionMergeRule
import org.bitlap.core.sql.rule.enumerable.BitlapEnumerableUnionRule

import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.rules.CoreRules

/** Entrance functions for sql rules.
 */

/** @see
 *    [org.apache.calcite.rel.rules.CoreRules]
 */
val RULES = List(
  // rules to transform calcite logical node plan
  List(CoreRules.UNION_MERGE),
  // rules to transform bitlap node plan
  List(
    BitlapRelConverter(),
    BitlapAggConverter(),
    BitlapFilterTableScanRule(),
    ValidRule(),
    BitlapTableConverter()
  ),
  List(BitlapRowTypeConverter())
)

/** @see
 *    [org.apache.calcite.tools.Programs.RULE_SET]
 *  @see
 *    [org.apache.calcite.adapter.enumerable.EnumerableRules.ENUMERABLE_RULES]
 */
val ENUMERABLE_RULES = List(
  BitlapEnumerableAggregateRule(),
  BitlapEnumerableAggregateSortedRule(),
  BitlapEnumerableProjectRule(),
  BitlapEnumerableFilterRule(),
  BitlapEnumerableUnionRule(),
  BitlapEnumerableUnionMergeRule()
)

extension (relNode: RelNode) {

  /** clean HepRelVertex wrapper
   */
  def clean(): RelNode = {
    relNode match
      case r: HepRelVertex => r.getCurrentRel
      case _               => relNode
  }

  /** inject parent node
   *
   *  @return
   *    parent node
   */
  def injectParent(parent: (RelNode) => RelNode): RelNode = {
    val p = parent(relNode)
    relNode match {
      case node: BitlapNode =>
        node.parent = p
      case r if r.isInstanceOf[HepRelVertex] && r.asInstanceOf[HepRelVertex].getCurrentRel.isInstanceOf[BitlapNode] =>
        r.asInstanceOf[HepRelVertex].getCurrentRel.asInstanceOf[BitlapNode].parent = p
    }
    p
  }
}

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

import org.bitlap.core.sql.rel.BitlapSort
import org.bitlap.core.sql.rel.BitlapUnion

import org.apache.calcite.adapter.enumerable.EnumerableMergeUnionRule

/** Convert merged BitlapUnion to enumerable rule.
 *
 *  @see
 *    [org.apache.calcite.adapter.enumerable.EnumerableMergeUnionRule]
 *  @see
 *    [EnumerableRules.ENUMERABLE_MERGE_UNION_RULE]
 */
class BitlapEnumerableUnionMergeRule extends EnumerableMergeUnionRule(BitlapEnumerableUnionMergeRule.CONFIG)

object BitlapEnumerableUnionMergeRule {

  val CONFIG: EnumerableMergeUnionRule.Config = EnumerableMergeUnionRule.Config.DEFAULT_CONFIG
    .withDescription("BitlapEnumerableUnionMergeRule")
    .withOperandSupplier(b0 =>
      b0.operand(classOf[BitlapSort]).oneInput(b1 => b1.operand(classOf[BitlapUnion]).anyInputs())
    )
    .`as`(classOf[EnumerableMergeUnionRule.Config])
}

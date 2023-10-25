/**
 * Copyright (C) 2023 bitlap.org .
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

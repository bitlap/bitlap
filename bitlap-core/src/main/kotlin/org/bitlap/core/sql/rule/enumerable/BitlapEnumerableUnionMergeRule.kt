/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.rule.enumerable

import org.apache.calcite.adapter.enumerable.EnumerableMergeUnionRule
import org.apache.calcite.adapter.enumerable.EnumerableRules
import org.bitlap.core.sql.rel.BitlapSort
import org.bitlap.core.sql.rel.BitlapUnion

/**
 * Convert merged BitlapUnion to enumerable rule.
 *
 * @see [org.apache.calcite.adapter.enumerable.EnumerableMergeUnionRule]
 * @see [EnumerableRules.ENUMERABLE_MERGE_UNION_RULE]
 */
class BitlapEnumerableUnionMergeRule : EnumerableMergeUnionRule(CONFIG) {

    companion object {
        private val CONFIG = Config.DEFAULT_CONFIG
            .withDescription("BitlapEnumerableUnionMergeRule")
            .withOperandSupplier { b0 ->
                b0.operand(BitlapSort::class.java).oneInput { b1 ->
                    b1.operand(BitlapUnion::class.java).anyInputs()
                }
            }
            .`as`(Config::class.java)
    }
}

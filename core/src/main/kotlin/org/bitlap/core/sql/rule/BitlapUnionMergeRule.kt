package org.bitlap.core.sql.rule

import cn.hutool.core.util.ReflectUtil
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.logical.LogicalUnion
import org.apache.calcite.rel.rules.UnionMergeRule
import org.bitlap.core.sql.rel.BitlapUnion

/**
 * merge [BitlapUnion]s to one union
 *
 * @see [org.apache.calcite.rel.rules.CoreRules.UNION_MERGE]
 */
class BitlapUnionMergeRule : UnionMergeRule(CONFIG) {

    companion object {
        private val CONFIG = Config.DEFAULT
            .withOperandFor(BitlapUnion::class.java)
            .withDescription("BitlapUnionMergeRule")
            .`as`(Config::class.java)
    }

    override fun onMatch(call: RelOptRuleCall) {
        super.onMatch(call)
        val res = ReflectUtil.getFieldValue(call, "results") as ArrayList<*>
        when (val rel = res.firstOrNull()) {
            is LogicalUnion -> {
                res.clear() // should clear to add new one below
                call.transformTo(BitlapUnion(rel.cluster, rel.traitSet, rel.inputs, rel.all))
            }
        }
    }
}

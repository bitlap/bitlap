package org.bitlap.core.sql.rule.enumerable

import org.apache.calcite.adapter.enumerable.EnumerableConvention
import org.apache.calcite.adapter.enumerable.EnumerableProject
import org.apache.calcite.adapter.enumerable.EnumerableRules
import org.apache.calcite.plan.Convention
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Project
import org.bitlap.core.sql.rel.BitlapProject
import org.bitlap.core.sql.rule.AbsRelRule

/**
 * Convert BitlapProject to enumerable rule.
 *
 * @see [org.apache.calcite.adapter.enumerable.EnumerableProjectRule]
 * @see [EnumerableRules.ENUMERABLE_PROJECT_RULE]
 */
class BitlapEnumerableProjectRule : AbsRelRule(CONFIG) {

    companion object {
        private val CONFIG = Config.INSTANCE.withConversion(
            BitlapProject::class.java,
            { !it.containsOver() },
            Convention.NONE, EnumerableConvention.INSTANCE,
            "BitlapEnumerableProjectRule"
        )
    }

    override fun convert0(rel: RelNode, call: RelOptRuleCall): RelNode? {
        val project = rel as Project
        return EnumerableProject.create(
            convert(
                project.input,
                project.input.traitSet.replace(EnumerableConvention.INSTANCE)
            ),
            project.projects,
            project.rowType,
        )
    }
}

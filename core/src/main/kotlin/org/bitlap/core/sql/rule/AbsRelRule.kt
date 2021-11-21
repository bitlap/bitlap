package org.bitlap.core.sql.rule

import org.apache.calcite.plan.Convention
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.TableScan
import org.bitlap.common.logger

abstract class AbsRelRule(config: Config) : ConverterRule(config) {

    protected val log = logger(this::class.java.simpleName)

    constructor(input: Class<out RelNode>, description: String) : this(
        Config.INSTANCE.withConversion(input, Convention.NONE, Convention.NONE, description)
    )

    override fun onMatch(call: RelOptRuleCall) {
        val rel = call.rel<RelNode>(0)
        if (rel.traitSet.contains(inTrait)) {
            val converted = convert0(rel, call)
            if (converted != null) {
                call.transformTo(converted)
            }
        }
    }

    protected abstract fun convert0(rel: RelNode, call: RelOptRuleCall): RelNode?

    final override fun convert(rel: RelNode): RelNode? {
        throw NotImplementedError("Deprecated by function convert0(RelNode, RelOptRuleCall).")
    }

    protected fun hasTableScanNode(rel: RelNode): Boolean {
        if (rel is TableScan) {
            return true
        }
        return rel.inputs.mapNotNull { it.clean() }.any { this.hasTableScanNode(it) }
    }
}

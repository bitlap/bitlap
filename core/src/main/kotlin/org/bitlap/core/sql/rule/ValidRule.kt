package org.bitlap.core.sql.rule

import org.apache.calcite.plan.Convention
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.logical.LogicalTableScan
import org.bitlap.common.logger

class ValidRule(config: Config) : ConverterRule(config) {

    private val log = logger { }

    companion object {
        val INSTANCE = ValidRule(
            Config.INSTANCE
                .withConversion(LogicalTableScan::class.java, Convention.NONE, Convention.NONE, "ValidRule")
        )
    }

    override fun convert(rel: RelNode): RelNode? {
        log.info("=================ValidRule===============")
        return null
    }
}

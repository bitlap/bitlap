package org.bitlap.core.sql.rule

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.RexUtil
import org.apache.calcite.util.mapping.Mappings
import org.bitlap.core.sql.rel.BitlapFilter
import org.bitlap.core.sql.rel.BitlapTableFilterScan
import org.bitlap.core.sql.rel.BitlapTableScan

/**
 * see [org.apache.calcite.rel.rules.FilterTableScanRule]
 */
class BitlapFilterTableScanRule : AbsRelRule(BitlapFilter::class.java, "BitlapFilterTableScanRule") {

    override fun convert0(rel: RelNode, call: RelOptRuleCall): RelNode {
        rel as BitlapFilter
        return when (val scan = rel.input.clean()) {
            is BitlapTableScan -> {
                val projects = scan.identity()
                val mapping = Mappings.target(projects, scan.table!!.rowType.fieldCount)
                val filter = RexUtil.apply(mapping.inverse(), rel.condition)
                // push down filter
                BitlapTableFilterScan(scan.cluster, scan.traitSet, scan.hints, scan.table, listOf(filter), rel.parent)
            }
            else -> rel
        }
    }
}

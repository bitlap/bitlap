package org.bitlap.core.sql.rule

import org.apache.calcite.plan.RelOptPredicateList
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexShuttle
import org.apache.calcite.rex.RexSimplify
import org.apache.calcite.rex.RexUtil
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.mapping.Mappings
import org.bitlap.core.sql.Keyword
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
                val relBuilder = call.builder()
                val rexBuilder = RexBuilder(relBuilder.typeFactory)
                val projects = scan.identity()
                val mapping = Mappings.target(projects, scan.table!!.rowType.fieldCount)
                val filter = RexUtil.apply(mapping.inverse(), rel.condition)

                // simplify filter, try to predict is the where expression is always false
                val oFilter = RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, RexUtil.EXECUTOR)
                    .simplifyUnknownAsFalse(filter)

                // if condition is always false, just push to [BitlapTableConverter] and return
                if (oFilter.isAlwaysFalse) {
                    BitlapTableFilterScan(
                        scan.cluster, scan.traitSet, scan.hints, scan.table,
                        oFilter, oFilter, rel.parent
                    )
                } else {
                    // push down filter
                    val timeFilter = this.pruneTimeFilter(oFilter, scan.rowType, rexBuilder)
                    BitlapTableFilterScan(
                        scan.cluster, scan.traitSet, scan.hints, scan.table,
                        timeFilter, RexUtil.toDnf(rexBuilder, oFilter), rel.parent
                    )
                }
            }
            else -> rel
        }
    }

    private fun pruneTimeFilter(filter: RexNode, rowType: RelDataType, rexBuilder: RexBuilder): RexNode {
        val otherFields = rowType.fieldList.filter { it.name != Keyword.TIME }
        val timeFilter = filter.accept(object : RexShuttle() {
            override fun visitCall(call: RexCall): RexNode? {
                return when (call.kind) {
                    SqlKind.AND,
                    SqlKind.OR -> {
                        val operands = mutableListOf<RexNode?>()
                            .also { visitList(call.operands, it) }
                            .filterNotNull()
                        if (operands.size == 2) {
                            rexBuilder.makeCall(call.op, operands)
                        } else {
                            operands.firstOrNull()
                        }
                    }
                    else -> {
                        var hasOther = false
                        call.accept(object : RexShuttle() {
                            override fun visitInputRef(inputRef: RexInputRef): RexNode {
                                hasOther = otherFields.any { inputRef.index - it.index == 0 }
                                return inputRef
                            }
                        })
                        if (hasOther) {
                            rexBuilder.makeLiteral(true) // other condition make always true
                        } else {
                            call
                        }
                    }
                }
            }
        })
        return timeFilter
    }
}

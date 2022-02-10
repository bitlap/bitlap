package org.bitlap.core.sql.rule

import org.apache.calcite.DataContexts
import org.apache.calcite.plan.RelOptPredicateList
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexExecutorImpl
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexShuttle
import org.apache.calcite.rex.RexSimplify
import org.apache.calcite.rex.RexUtil
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.mapping.Mappings
import org.bitlap.core.sql.Keyword
import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.PruneTimeFilter
import org.bitlap.core.sql.rel.BitlapFilter
import org.bitlap.core.sql.rel.BitlapTableFilterScan
import org.bitlap.core.sql.rel.BitlapTableScan
import org.bitlap.core.sql.rule.shuttle.RexInputRefShuttle

/**
 * see [org.apache.calcite.rel.rules.FilterTableScanRule]
 */
class BitlapFilterTableScanRule : AbsRelRule(BitlapFilter::class.java, "BitlapFilterTableScanRule") {

    override fun convert0(rel: RelNode, call: RelOptRuleCall): RelNode {
        rel as BitlapFilter
        return when (val scan = rel.input.clean()) {
            is BitlapTableFilterScan -> rel // has been converted
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
                        PruneTimeFilter(), PrunePushedFilter(), true, rel.parent
                    )
                }
                // push down filter
                else {
                    val dnfFilter = RexUtil.toDnf(rexBuilder, oFilter)
                    // prune pure filter to push down, and retain the rest
                    // current not support OR expression
                    val (timeFilter, pruneFilter, unPruneFilter) = this.pruneFilter(dnfFilter, scan.rowType, rexBuilder)
                    val inputScan = BitlapTableFilterScan(
                        scan.cluster, scan.traitSet, scan.hints, scan.table,
                        timeFilter, pruneFilter, false, rel.parent
                    )
                    if (unPruneFilter == null) {
                        inputScan
                    } else {
                        rel.copy(rel.traitSet, inputScan, unPruneFilter)
                    }
                }
            }
            else -> rel
        }
    }

    private fun pruneFilter(
        filter: RexNode,
        rowType: RelDataType,
        rexBuilder: RexBuilder
    ): Triple<PruneTimeFilter, PrunePushedFilter, RexNode?> {
        val timeFilter = PruneTimeFilter()
        val timeField = rowType.fieldList.first { it.name == Keyword.TIME }

        val pruneFilter = PrunePushedFilter()
        val fieldIndexMap = rowType.fieldList.groupBy { it.index }.mapValues { it.value.first() }

        val unPruneFilter = filter.accept(object : RexShuttle() {
            override fun visitCall(call: RexCall): RexNode? {
                return when (call.kind) {
                    SqlKind.OR -> TODO("OR expression is not supported now.")
                    SqlKind.AND -> {
                        val operands = mutableListOf<RexNode?>()
                            .also { visitList(call.operands, it) }
                            .filterNotNull()
                        if (operands.size == 2) {
                            rexBuilder.makeCall(call.op, operands)
                        } else {
                            operands.firstOrNull()
                        }
                    }
                    // x (=, <>, >, >=, <, <=) xxx
                    in SqlKind.COMPARISON -> {
                        val (left, right) = call.operands
                        when {
                            left is RexInputRef && right is RexLiteral -> {
                                val inputField = fieldIndexMap[left.index]!!
                                if (timeField == inputField) {
                                    timeFilter.add(
                                        inputField.name,
                                        resolveFilter(call, inputField, rowType, rexBuilder),
                                        call.toString().replace("$${inputField.index}", inputField.name)
                                    )
                                } else {
                                    pruneFilter.add(
                                        inputField.name, call.kind.sql.lowercase(),
                                        right.getValueAs(String::class.java)!!,
                                        resolveFilter(call, inputField, rowType, rexBuilder),
                                        call.toString().replace("$${inputField.index}", inputField.name)
                                    )
                                }
                                null
                            }
                            else -> {
                                val refs = RexInputRefShuttle.of(call).getInputRefs()
                                if (refs.size == 1) {
                                    val inputField = fieldIndexMap[refs.first().index]!!
                                    if (timeField == inputField) {
                                        timeFilter.add(
                                            inputField.name,
                                            resolveFilter(call, inputField, rowType, rexBuilder),
                                            call.toString().replace("$${inputField.index}", inputField.name)
                                        )
                                        null
                                    } else {
                                        call
                                    }
                                } else {
                                    call
                                }
                            }
                        }
                    }
                    // x in (xx, xxx, ...)
                    SqlKind.SEARCH -> {
                        TODO()
                    }
                    // x is [not] null
                    SqlKind.IS_NULL, SqlKind.IS_NOT_NULL -> TODO()
                    // x like xxx
                    SqlKind.LIKE -> TODO()
                    else -> call
                }
            }
        })
        return Triple(timeFilter, pruneFilter, unPruneFilter)
    }

    /**
     * resolve time filter to normal function
     */
    inline fun <reified T> resolveFilter(
        inputFilter: RexNode,
        inputField: RelDataTypeField,
        rowType: RelDataType,
        builder: RexBuilder
    ): (T) -> Boolean {
        // reset field index to 0
        val filter = RexUtil.apply(
            Mappings.target(
                mapOf(inputField.index to 0),
                inputField.index + 1, 1
            ),
            inputFilter
        )
        // convert to normal function
        val executor = RexExecutorImpl.getExecutable(builder, listOf(filter), rowType).function
        return {
            // why inputRecord? see DataContextInputGetter
            val input = DataContexts.of(mapOf("inputRecord" to arrayOf(it)))
            (executor.apply(input) as Array<*>).first() as Boolean
        }
    }
}

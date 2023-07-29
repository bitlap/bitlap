/**
 * Copyright (C) 2023 bitlap.org .
 */
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
import org.apache.calcite.util.NlsString
import org.apache.calcite.util.RangeSets
import org.apache.calcite.util.Sarg
import org.apache.calcite.util.mapping.Mappings
import org.bitlap.core.sql.FilterOp
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
                    // x in (xx, xxx, ...)
                    // x between xx and xxx
                    in SqlKind.COMPARISON, SqlKind.SEARCH -> {
                        val (left, right) = call.operands
                        when {
                            left is RexInputRef && right is RexLiteral || left is RexLiteral && right is RexInputRef -> {
                                val (ref, refValue) = if (left is RexInputRef) left to right else right to left
                                ref as RexInputRef
                                refValue as RexLiteral

                                val inputField = fieldIndexMap[ref.index]!!
                                if (timeField == inputField) {
                                    timeFilter.add(
                                        inputField.name,
                                        resolveFilter(call, inputField, rowType, rexBuilder),
                                        call.toString().replace("$${inputField.index}", inputField.name)
                                    )
                                } else {
                                    val (values, op) = getLiteralValue(refValue, call.kind.sql.lowercase())
                                    pruneFilter.add(
                                        inputField.name, op,
                                        values,
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

    @Suppress("UNCHECKED_CAST", "UnstableApiUsage")
    private fun getLiteralValue(literal: RexLiteral, op: String): Pair<List<String>, FilterOp> {
        return when (literal.value) {
            is Sarg<*> -> {
                val values = mutableListOf<NlsString>()
                var filterOp = FilterOp.EQUALS
                RangeSets.forEach(
                    (literal.value as Sarg<NlsString>).rangeSet,
                    object : RangeSets.Consumer<NlsString> {
                        override fun all() = throw IllegalStateException("Illegal compare all.")
                        override fun atLeast(lower: NlsString) {
                            values.add(lower)
                            filterOp = FilterOp.GREATER_EQUALS_THAN
                        }

                        override fun atMost(upper: NlsString) {
                            values.add(upper)
                            filterOp = FilterOp.LESS_EQUALS_THAN
                        }

                        override fun greaterThan(lower: NlsString) {
                            values.add(lower)
                            filterOp = FilterOp.GREATER_THAN
                        }

                        override fun lessThan(upper: NlsString) {
                            values.add(upper)
                            filterOp = FilterOp.LESS_THAN
                        }

                        override fun singleton(value: NlsString) {
                            values.add(value)
                            filterOp = FilterOp.EQUALS
                        }

                        override fun closed(lower: NlsString, upper: NlsString) {
                            values.add(lower)
                            values.add(upper)
                            filterOp = FilterOp.CLOSED
                        }

                        override fun closedOpen(lower: NlsString, upper: NlsString) {
                            values.add(lower)
                            values.add(upper)
                            filterOp = FilterOp.CLOSED_OPEN
                        }

                        override fun openClosed(lower: NlsString, upper: NlsString) {
                            values.add(lower)
                            values.add(upper)
                            filterOp = FilterOp.OPEN_CLOSED
                        }

                        override fun open(lower: NlsString, upper: NlsString) {
                            values.add(lower)
                            values.add(upper)
                            filterOp = FilterOp.OPEN
                        }
                    }
                )
                values.map { it.value } to filterOp
            }
            else ->
                listOf(literal.getValueAs(String::class.java)!!) to FilterOp.from(op)
        }
    }
}

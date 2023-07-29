/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.rule

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexShuttle
import org.apache.calcite.sql.type.BasicSqlType
import org.bitlap.core.sql.rel.BitlapAggregate
import org.bitlap.core.sql.rel.BitlapNode
import org.bitlap.core.sql.rel.BitlapProject

/**
 * convert row any type to actually type from child node
 */
class BitlapRowTypeConverter : AbsRelRule(BitlapNode::class.java, "BitlapRowTypeConverter") {

    override fun convert0(rel: RelNode, call: RelOptRuleCall): RelNode {
        rel as BitlapNode
        // convert root node
        val parent = rel.parent
        if (parent != null || !hasAnyType(rel.rowType)) {
            return rel
        }
        return this.convert00(rel, call)
    }

    private fun convert00(rel: RelNode, call: RelOptRuleCall): RelNode {
        return when {
            rel is HepRelVertex -> {
                this.convert00(rel.currentRel, call)
            }
            !hasAnyType(rel.rowType) -> {
                rel.inputs.forEachIndexed { i, n ->
                    rel.replaceInput(i, this.convert00(n, call))
                }
                rel
            }
            rel is BitlapProject -> {
                this.convertProjectType(rel, this.convert00(rel.input, call))
            }
            rel is BitlapAggregate -> {
                val i = this.convert00(rel.input, call)
                val agg = rel.aggCallList.map {
                    AggregateCall.create(
                        it.aggregation, it.isDistinct, it.isApproximate, it.ignoreNulls(),
                        it.argList, it.filterArg, it.distinctKeys,
                        it.collation, rel.groupCount, i, null, it.name
                    )
                }
                rel.copy(i, agg)
            }
            else -> {
                rel.inputs.forEachIndexed { i, n ->
                    rel.replaceInput(i, this.convert00(n, call))
                }
                rel
            }
        }
    }

    private fun hasAnyType(rowType: RelDataType): Boolean {
        return when (rowType) {
            is BasicSqlType -> {
                rowType.sqlTypeName.name.uppercase() == "ANY"
            }
            else -> {
                rowType.fieldList.any { it.type.sqlTypeName.name.uppercase() == "ANY" }
            }
        }
    }

    // convert project rowType and expr type
    private fun convertProjectType(rel: BitlapProject, newInput: RelNode): BitlapProject {
        val builder = rel.cluster.typeFactory.builder()
        val inputRowType = rel.input.rowType
        val outputRowType = rel.rowType
        val newProjects = outputRowType.fieldList.mapIndexed { idx, rt ->
            val refIndex = mutableListOf<Int>()
            val newProject = rel.projects[idx].accept(object : RexShuttle() {
                override fun visitInputRef(inputRef: RexInputRef): RexNode {
                    refIndex.add(inputRef.index)
                    val newRef = when {
                        hasAnyType(inputRef.type) ->
                            RexInputRef.of(inputRef.index, inputRowType)
                        else ->
                            inputRef
                    }
                    return super.visitInputRef(newRef)
                }
            })
            if (hasAnyType(rt.type) && refIndex.isNotEmpty()) {
                builder.add(inputRowType.fieldList[refIndex.first()])
            } else {
                builder.add(rt)
            }
            newProject
        }
        return rel.copy(newInput, builder.build(), newProjects)
    }
}

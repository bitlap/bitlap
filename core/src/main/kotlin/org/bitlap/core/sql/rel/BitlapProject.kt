package org.bitlap.core.sql.rel

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexShuttle

/**
 * Project logical plan, see [org.apache.calcite.rel.logical.LogicalProject]
 */
class BitlapProject(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    hints: List<RelHint>,
    input: RelNode,
    projects: List<RexNode>,
    rowType: RelDataType,
    override var parent: RelNode? = null,
) : Project(cluster, traitSet, hints, input, projects, rowType), BitlapNode {

    override fun copy(
        traitSet: RelTraitSet,
        input: RelNode,
        projects: MutableList<RexNode>,
        rowType: RelDataType,
    ): BitlapProject {
        return BitlapProject(cluster, traitSet, hints, input, projects, rowType, parent)
    }

    override fun withHints(hintList: List<RelHint>): RelNode {
        return BitlapProject(cluster, traitSet, hintList, input, projects, getRowType())
    }

    override fun deepHashCode(): Int {
        return super.deepHashCode0()
    }

    override fun deepEquals(obj: Any?): Boolean {
        return super.deepEquals0(obj)
    }

    companion object {
        fun of(
            cluster: RelOptCluster,
            traitSet: RelTraitSet,
            hints: List<RelHint>,
            input: RelNode,
            projects: List<RexNode>,
            rowType: RelDataType,
            parent: RelNode? = null
        ): BitlapProject {
            val fixRowType = this.fixRowType(cluster.typeFactory, projects, rowType, input.rowType)
            return BitlapProject(cluster, traitSet, hints, input, projects, fixRowType, parent)
        }

        // fix any type
        fun fixRowType(
            typeFactory: RelDataTypeFactory,
            projects: List<RexNode>,
            currentRowType: RelDataType,
            inputRowType: RelDataType
        ): RelDataType {
            val builder = typeFactory.builder()
            currentRowType.fieldList.forEachIndexed { idx, rt ->
                val refIndex = mutableListOf<Int>()
                projects[idx].accept(object : RexShuttle() {
                    override fun visitInputRef(inputRef: RexInputRef): RexNode {
                        refIndex.add(inputRef.index)
                        return super.visitInputRef(inputRef)
                    }
                })
                if (rt.type.sqlTypeName.name.uppercase() == "ANY" && refIndex.isNotEmpty()) {
                    builder.add(inputRowType.fieldList[refIndex.first()])
                } else {
                    builder.add(rt)
                }
            }
            return builder.build()
        }
    }
}

package org.bitlap.core.sql.rel

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rex.RexNode

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
    ): Project {
        return BitlapProject(cluster, traitSet, hints, input, projects, rowType, parent)
    }

    override fun withHints(hintList: List<RelHint>): RelNode {
        return LogicalProject(cluster, traitSet, hintList, input, projects, getRowType())
    }

    override fun deepHashCode(): Int {
        return super.deepHashCode0()
    }

    override fun deepEquals(obj: Any?): Boolean {
        return super.deepEquals0(obj)
    }
}

package org.bitlap.core.sql.rel

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rex.RexNode

/**
 * Bitlap table scan with push down filters
 */
class BitlapTableFilterScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    hints: List<RelHint>,
    table: RelOptTable?,
    val filters: List<RexNode>,
    override var parent: RelNode? = null,
) : BitlapTableScan(cluster, traitSet, hints, table, parent), BitlapNode {

    override fun explainTerms(pw: RelWriter): RelWriter {
        return super.explainTerms(pw)
            .item("filters", filters)
    }

    override fun withHints(hintList: MutableList<RelHint>): RelNode {
        return BitlapTableFilterScan(cluster, traitSet, hintList, table, filters, parent)
    }

    override fun withTable(table: RelOptTable): BitlapTableFilterScan {
        return BitlapTableFilterScan(cluster, traitSet, hints, table, filters, parent)
    }

    override fun copy(traitSet: RelTraitSet?, inputs: MutableList<RelNode>?): RelNode {
        return this
    }
}

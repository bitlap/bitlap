package org.bitlap.core.sql.rel

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.prepare.RelOptTableImpl
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.hint.RelHint
import org.bitlap.core.sql.table.BitlapSqlQueryTable

/**
 * Table scan logical plan, see [org.apache.calcite.rel.logical.LogicalTableScan]
 */
open class BitlapTableScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    hints: List<RelHint>,
    table: RelOptTable?,
    override var parent: RelNode? = null,
) : TableScan(cluster, traitSet, hints, table), BitlapNode {

    open var converted: Boolean = false

    fun getOTable(): BitlapSqlQueryTable {
        return (this.table as RelOptTableImpl).table() as BitlapSqlQueryTable
    }

    override fun explainTerms(pw: RelWriter): RelWriter {
        return super.explainTerms(pw)
            .item("class", getOTable()::class.java.simpleName)
    }

    override fun withHints(hintList: MutableList<RelHint>): RelNode {
        return BitlapTableScan(cluster, traitSet, hintList, table, parent)
    }

    open fun withTable(table: RelOptTable): BitlapTableScan {
        return BitlapTableScan(cluster, traitSet, hints, table, parent)
    }

    override fun copy(traitSet: RelTraitSet?, inputs: MutableList<RelNode>?): RelNode {
        return this
    }
}

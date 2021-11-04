package org.bitlap.core.sql.rel

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.Convention
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.prepare.RelOptTableImpl
import org.apache.calcite.rel.RelCollationTraitDef
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.schema.Table
import org.bitlap.core.sql.table.BitlapSqlQueryTable

/**
 * Table scan logical plan, see [org.apache.calcite.rel.logical.LogicalTableScan]
 */
class BitlapTableScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    hints: List<RelHint>,
    table: RelOptTable,
) : TableScan(cluster, traitSet, hints, table) {

    companion object {

        fun create(cluster: RelOptCluster, relOptTable: RelOptTable, hints: List<RelHint>): BitlapTableScan {
            val table = relOptTable.unwrap(Table::class.java)
            val traitSet = cluster.traitSetOf(Convention.NONE)
                .replaceIfs(RelCollationTraitDef.INSTANCE) {
                    if (table != null) {
                        table.statistic.collations
                    } else {
                        ImmutableList.of()
                    }
                }
            return BitlapTableScan(cluster, traitSet, hints, relOptTable)
        }
    }

    fun getOTable(): BitlapSqlQueryTable {
        return (this.table as RelOptTableImpl).table() as BitlapSqlQueryTable
    }

    override fun explainTerms(pw: RelWriter): RelWriter {
        return super.explainTerms(pw)
            .item("class", getOTable()::class.java.simpleName)
    }
}

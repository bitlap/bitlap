package org.bitlap.core.sql.rule

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.Convention
import org.apache.calcite.prepare.RelOptTableImpl
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.sql.SqlSelect
import org.bitlap.core.sql.rel.BitlapTableScan
import org.bitlap.core.sql.table.BitlapSqlQueryMetricTable
import org.bitlap.core.sql.table.BitlapSqlQueryTable

class BitlapTableConverter(config: Config) : ConverterRule(config) {

    companion object {
        val INSTANCE = BitlapTableConverter(
            Config.INSTANCE
                .withConversion(LogicalTableScan::class.java, Convention.NONE, Convention.NONE, "BitlapTableConverter")
        )
    }

    override fun convert(rel: RelNode): RelNode {
        rel as LogicalTableScan
        val optTable = rel.table as RelOptTableImpl
        val oTable = optTable.table() as BitlapSqlQueryTable
        // get dimensions
        // val mayCols = oTable.mayCols
        // val dimensions = this.resolveDimensions(oTable.selectNode)

        val target = BitlapSqlQueryMetricTable(oTable.table)
        return BitlapTableScan.create(
            rel.cluster,
            RelOptTableImpl.create(
                optTable.relOptSchema,
                optTable.rowType,
                target,
                optTable.qualifiedName as ImmutableList
            ),
            rel.hints,
        )
    }

    private fun resolveDimensions(select: SqlSelect): Set<String> {
        val result = mutableSetOf<String>()
        // group list
//        select.group?.forEach {
//
//        }
        return result
    }
}

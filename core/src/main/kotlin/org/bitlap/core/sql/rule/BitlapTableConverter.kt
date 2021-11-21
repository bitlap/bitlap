package org.bitlap.core.sql.rule

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.prepare.RelOptTableImpl
import org.apache.calcite.rel.RelNode
import org.bitlap.core.sql.rel.BitlapTableFilterScan
import org.bitlap.core.sql.rel.BitlapTableScan
import org.bitlap.core.sql.table.BitlapSqlQueryMetricTable
import org.bitlap.core.sql.table.BitlapSqlQueryTable

class BitlapTableConverter : AbsRelRule(BitlapTableScan::class.java, "BitlapTableConverter") {

    override fun convert0(rel: RelNode, call: RelOptRuleCall): RelNode {
        rel as BitlapTableScan
        if (rel.converted) {
            return rel
        }
        val optTable = rel.table as RelOptTableImpl
        val oTable = optTable.table() as BitlapSqlQueryTable
        val analyzer = oTable.analyzer

        // get filters
        val filters = when (rel) {
            is BitlapTableFilterScan -> rel.filters
            else -> emptyList()
        }
        val target = when {
            analyzer.hasNoTimeInQuery() ->
                BitlapSqlQueryMetricTable(oTable.table, oTable.analyzer, filters)
            else ->
                // TODO: with dimensions
                throw NotImplementedError()
        }
        return rel.withTable(
            RelOptTableImpl.create(
                optTable.relOptSchema,
                optTable.rowType,
                target,
                optTable.qualifiedName as ImmutableList
            )
        ).also { it.converted = true }
    }
}

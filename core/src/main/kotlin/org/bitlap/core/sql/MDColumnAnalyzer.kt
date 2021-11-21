package org.bitlap.core.sql

import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.sql.SqlAsOperator
import org.apache.calcite.sql.SqlBasicCall
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlSelect
import org.bitlap.core.data.metadata.Table

/**
 * analyzer for [MDColumn] from [SqlSelect]
 */
class MDColumnAnalyzer(val table: Table, val select: SqlSelect) {

    private var mdColumnMap: Map<String, MDColumn>
    private var mdColumnNames: List<String>
    private var mdColumns: List<MDColumn>

    init {
        this.mdColumnMap = this.analyseMDColumns(select)
        this.mdColumnNames = this.mdColumnMap.keys.toList()
        this.mdColumns = this.mdColumnMap.values.toList()
    }

    /**
     * get metric & dimension columns from sql node
     */
    private fun analyseMDColumns(node: SqlSelect): Map<String, MDColumn> {
        val cols = mutableMapOf<String, MDColumn>()
        // select
        node.selectList.forEach {
            this.getColumnName(it).map { c ->
                if (cols.contains(c.name)) {
                    cols[c.name]!!.checkType(c.type)
                        .withProject(true)
                        .withAgg(c.aggs)
                } else {
                    cols[c.name] = c.withProject(true)
                }
            }
        }
        // from: ignore
        // where: treated as dimensions currently
        node.where?.also {
            this.getColumnName(it).map { c ->
                if (cols.contains(c.name)) {
                    cols[c.name]!!.checkType(c.type)
                        .withFilter(true)
                } else {
                    cols[c.name] = c.withFilter(true)
                }
            }
        }
        // group by: treated as dimensions
        node.group?.forEach {
            this.getColumnName(it).map { c ->
                if (cols.contains(c.name)) {
                    cols[c.name]!!.checkType(c.type)
                } else {
                    cols[c.name] = c
                }
            }
        }

        return cols
    }

    private fun getColumnName(node: SqlNode): List<MDColumn> {
        return when (node) {
            is SqlIdentifier -> {
                if (node.isStar) {
                    throw IllegalArgumentException("It is forbidden to query * from ${table.database}.${table.name}")
                }
                listOf(MDColumn(node.names.last(), DimensionCol))
            }
            is SqlBasicCall -> {
                when (node.operator) {
                    is SqlAsOperator -> {
                        getColumnName(node.operandList.first())
                    }
                    is SqlAggFunction -> {
                        node.operandList.flatMap(::getColumnName).map {
                            val operator = node.operator as SqlAggFunction
                            MDColumn(it.name, MetricCol)
                                .withAgg(operator to (node.functionQuantifier?.toValue() ?: ""))
                        }
                    }
                    else -> {
                        node.operandList.flatMap(::getColumnName)
                    }
                }
            }
            is SqlLiteral -> emptyList()
            else -> throw IllegalArgumentException("Illegal node: $node")
        }
    }

    fun getMetricColNames() = this.mdColumns.filter { it.type is MetricCol }.map { it.name }.sorted().distinct()
    fun getDimensionColNames() = this.mdColumns.filter { it.type is DimensionCol }.map { it.name }.sorted().distinct()
    fun getFilterColNames() = this.mdColumns.filter { it.filter }.map { it.name }.sorted().distinct()

    /**
     * no time dimension in query
     */
    fun hasNoTimeInQuery() = this.mdColumns
        .none { it.project && it.type is DimensionCol && it.name != Keyword.TIME }
}

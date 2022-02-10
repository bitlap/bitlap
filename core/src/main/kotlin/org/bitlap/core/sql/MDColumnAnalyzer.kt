package org.bitlap.core.sql

import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.sql.SqlAsOperator
import org.apache.calcite.sql.SqlBasicCall
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlNodeList
import org.apache.calcite.sql.SqlSelect
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.mdm.format.DataType
import org.bitlap.core.mdm.format.DataTypeBBM
import org.bitlap.core.mdm.format.DataTypeCBM
import org.bitlap.core.mdm.format.DataTypeRBM
import org.bitlap.core.mdm.format.DataTypeRowValueMeta

/**
 * analyzer for [MDColumn] from [SqlSelect]
 */
class MDColumnAnalyzer(val table: Table, val select: SqlSelect) {

    private var mdColumnMap: Map<String, MDColumn>
    private var mdColumnNames: List<String>
    private var mdColumns: List<MDColumn>

    init {
        this.mdColumnMap = this.analyseMDColumns(select)
        this.mdColumnNames = this.mdColumnMap.keys.toList().sorted()
        this.mdColumns = this.mdColumnMap.values.toList().sortedBy { it.name }
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
                        .withAgg(c.aggregates)
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
            if (it is SqlIdentifier) {
                cols[it.names.last()]!!.withPure(true)
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
            is SqlNodeList -> {
                node.list.filterNotNull().flatMap(::getColumnName)
            }
            is SqlLiteral -> emptyList()
            else -> throw IllegalArgumentException("Illegal node: $node")
        }
    }

    fun getMetricColNames() = this.mdColumns.filter { it.type is MetricCol }.map { it.name }.distinct()
    fun getDimensionColNames() = this.mdColumns.filter { it.type is DimensionCol }.map { it.name }.distinct()
    fun getFilterColNames() = this.mdColumns.filter { it.filter }.map { it.name }.distinct()
    fun getDimensionColNamesWithoutTime() = this.getDimensionColNames().filter { it != Keyword.TIME }

    /**
     * none or one other dimension
     */
    fun hasNoOrOneOtherDim() = this.mdColumns
        .filter { it.type is DimensionCol && it.name != Keyword.TIME }
        .size <= 1

    /**
     * check if one metric should materialize
     */
    fun shouldMaterialize(metricName: String): Boolean {
        val metric = this.mdColumnMap[metricName]!!
        val dimensions = this.getDimensionColNames()

        // check whether the metric is Cartesian Product
        if (this.shouldCartesian()) {
            return true
        }

        // no dimensions includes time
        // 1) distinct aggregation: true
        // 2) sum, count aggregation: false
        if (dimensions.isEmpty()) {
            if (metric.isDistinct()) {
                return true
            }
            return false
        }

        // has no pure dimensions includes time
        // 1) dimension is complex expression
        // 2) dimension is not in group by
        val hasNoPureDims = this.mdColumns.any { it.type is DimensionCol && !it.pure }
        if (hasNoPureDims && metric.isDistinct()) {
            return true
        }
        return false
    }

    /**
     * check whether the metric is Cartesian Product
     */
    fun shouldCartesian(): Boolean {
        // there are more than or equal to 2 dimensions except time
        val noTimeDims = this.getDimensionColNamesWithoutTime()
        if (noTimeDims.size >= 2) {
            return true
        }
        return false
    }

    /**
     * get metric materialize type
     */
    fun materializeType(metricName: String, dimension: Int = 0): DataType {
        val metric = this.mdColumnMap[metricName]!!
        val materialize = this.shouldMaterialize(metricName)

        // not materialize
        if (!materialize) {
            return DataTypeRowValueMeta(metricName, -1)
        }

        //  materialize & Cartesian Product
        if (this.shouldCartesian()) {
            if (metric.isSum()) {
                return when (dimension) {
                    0 -> DataTypeCBM(metricName, -1)
                    else -> DataTypeBBM(metricName, -1)
                }
            }
            if (metric.isDistinct()) {
                return DataTypeRBM(metricName, -1)
            }
        }

        // materialize & not Cartesian Product
        if (metric.isSum() && metric.isDistinct()) {
            return DataTypeCBM(metricName, -1)
        }
        if (metric.isDistinct()) {
            return DataTypeRBM(metricName, -1)
        }
        throw IllegalArgumentException("Invalid input metric type, metric is $metric")
    }
}

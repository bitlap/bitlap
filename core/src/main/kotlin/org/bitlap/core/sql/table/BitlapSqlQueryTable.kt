package org.bitlap.core.sql.table

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.rex.RexNode
import org.apache.calcite.schema.ProjectableFilterableTable
import org.apache.calcite.schema.ScannableTable
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql.type.SqlTypeName
import org.bitlap.common.exception.BitlapException
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.sql.MDColumnAnalyzer
import org.bitlap.core.sql.QueryContext

/**
 * Desc: common bitlap table
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/9/12
 */
open class BitlapSqlQueryTable(open val table: Table) : AbstractTable(), ProjectableFilterableTable, ScannableTable {

    internal open val analyzer: MDColumnAnalyzer by lazy {
        MDColumnAnalyzer(table, QueryContext.get().currentSelectNode!!) // must not be null
    }

    /**
     * Returns this table's row type.
     */
    override fun getRowType(typeFactory: RelDataTypeFactory): RelDataType {
        val builder = typeFactory.builder()
        analyzer.getMetricColNames().forEach {
            builder.add(it, SqlTypeName.DOUBLE)
        }
        analyzer.getDimensionColNames().forEach {
            builder.add(it, SqlTypeName.VARCHAR)
        }
        return builder.build()
    }

    override fun scan(root: DataContext, filters: MutableList<RexNode>, projects: IntArray?): Enumerable<Array<Any?>> {
        throw BitlapException("You need to implement this method in a subclass.")
    }

    override fun scan(root: DataContext): Enumerable<Array<Any?>> {
        return this.scan(root, mutableListOf(), null)
    }
}

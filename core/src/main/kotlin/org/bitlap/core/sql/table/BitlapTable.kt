package org.bitlap.core.sql.table

import org.apache.calcite.DataContext
import org.apache.calcite.adapter.java.AbstractQueryableTable
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.linq4j.Linq4j
import org.apache.calcite.linq4j.QueryProvider
import org.apache.calcite.linq4j.Queryable
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.rex.RexNode
import org.apache.calcite.schema.ProjectableFilterableTable
import org.apache.calcite.schema.ScannableTable
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.SqlAsOperator
import org.apache.calcite.sql.SqlBasicCall
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlSelect
import org.apache.calcite.sql.type.SqlTypeName

/**
 * Desc common bitlap table
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/9/12
 */
open class BitlapTable() : AbstractQueryableTable(Object::class.java), ProjectableFilterableTable, ScannableTable {

    companion object {
        val query = ThreadLocal<SqlSelect>()
    }

    protected val mayCols = mutableSetOf<String>()

    /**
     * Returns this table's row type.
     */
    @Synchronized
    override fun getRowType(typeFactory: RelDataTypeFactory): RelDataType {
        if (query.get() != null) {
            mayCols.addAll(getColNames(query.get()))
            mayCols.removeIf { it.isBlank() }
        }
        val builder = typeFactory.builder()
        mayCols.forEach {
            builder.add(it, SqlTypeName.VARCHAR)
        }
        return builder.build()
    }

    private fun getColNames(node: SqlNode?): Set<String> {
        return when {
            node is SqlSelect -> {
                mutableSetOf<String>().apply {
                    addAll(node.selectList.flatMap(::getColNames))
                    addAll(getColNames(node.where))
                    addAll(node.group?.flatMap(::getColNames) ?: emptySet())
                    addAll(getColNames(node.having))
                    addAll(getColNames(node.from))
                }
            }
            node is SqlIdentifier && !node.isStar -> setOf(node.names.last())
            node is SqlBasicCall -> {
                if (node.operator is SqlAsOperator) {
                    getColNames(node.operands.first())
                } else {
                    node.operands.flatMap(::getColNames).toSet()
                }
            }
            else -> emptySet()
        }
    }

    /** Converts this table into a [Queryable].  */
    override fun <T : Any?> asQueryable(queryProvider: QueryProvider, schema: SchemaPlus, tableName: String): Queryable<T> {
        TODO("Not yet implemented")
    }

    override fun scan(root: DataContext, filters: MutableList<RexNode>, projects: IntArray?): Enumerable<Array<Any>>? {
        return Linq4j.asEnumerable(arrayOf(arrayOf("aa", "12"), arrayOf("aa", "mimosa")))
    }

    override fun scan(root: DataContext): Enumerable<Array<Any>> {
        return Linq4j.asEnumerable(arrayOf(arrayOf("aa", "12")))
    }
}

package org.bitlap.core.sql.table

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.rex.RexNode
import org.apache.calcite.schema.ProjectableFilterableTable
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql.SqlAsOperator
import org.apache.calcite.sql.SqlBasicCall
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlSelect
import org.apache.calcite.sql.type.SqlTypeName
import org.bitlap.common.exception.BitlapException
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.sql.QueryContext
import java.util.TreeSet

/**
 * Desc: common bitlap table
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/9/12
 */
open class BitlapSqlQueryTable(open val table: Table) : AbstractTable(), ProjectableFilterableTable {

    internal val selectNode: SqlSelect by lazy {
        QueryContext.get().currentSelectNode!! // must not be null
    }
    internal val mayCols = TreeSet<String>()

    /**
     * Returns this table's row type.
     */
    override fun getRowType(typeFactory: RelDataTypeFactory): RelDataType {
        mayCols.addAll(getColNames(selectNode))
        mayCols.removeIf { it.isBlank() }
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
                    addAll(getColNames(node.from))
                    addAll(getColNames(node.where))
                    addAll(node.group?.flatMap(::getColNames) ?: emptySet())
                    addAll(getColNames(node.having))
                    addAll(node.selectList.flatMap(::getColNames))
                    addAll(node.orderList?.flatMap(::getColNames) ?: emptySet())
                }
            }
            node is SqlIdentifier && !node.isStar -> setOf(node.names.last())
            node is SqlBasicCall -> {
                if (node.operator is SqlAsOperator) {
                    getColNames(node.operandList.first())
                } else {
                    node.operandList.flatMap(::getColNames).toSet()
                }
            }
            else -> emptySet()
        }
    }

    override fun scan(root: DataContext, filters: MutableList<RexNode>, projects: IntArray?): Enumerable<Array<Any?>> {
        throw BitlapException("You need to implement this method in a subclass.")
    }
}

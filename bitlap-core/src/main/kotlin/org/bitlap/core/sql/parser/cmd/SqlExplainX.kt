/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.sql.parser.cmd

import org.apache.calcite.DataContext
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlSelect
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlTypeName
import org.bitlap.core.BitlapContext
import org.bitlap.core.SessionId
import org.bitlap.core.sql.parser.BitlapSqlDdlNode

/**
 * Desc: explain a sql query
 */
class SqlExplainX(
    override val pos: SqlParserPos,
    private val stmt: SqlNode,
) : BitlapSqlDdlNode(pos, OPERATOR, listOf(stmt)) {

    companion object {
        val OPERATOR = SqlSpecialOperator("EXPLAIN", SqlKind.OTHER)
    }

    override val resultTypes: List<Pair<String, SqlTypeName>>
        get() = listOf(
            "result" to SqlTypeName.VARCHAR,
        )

    override fun operator(sessionId: SessionId, context: DataContext): List<Array<Any?>> {
        return when (stmt) {
            is SqlSelect -> {
                val plan = BitlapContext.sqlPlanner.parse(stmt.toString(), BitlapContext.getSession(sessionId))
                listOf(arrayOf(plan.explain()))
            }
            else -> {
                throw IllegalArgumentException("Illegal explain SqlSelect node $stmt")
            }
        }
    }
}

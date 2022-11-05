/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.sql.parser.ddl

import org.apache.calcite.DataContext
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlTypeName
import org.bitlap.common.exception.BitlapException
import org.bitlap.core.SessionId
import org.bitlap.core.sql.parser.BitlapSqlDdlNode

/**
 *    Parse tree for `USE database` statement.
 */
class SqlUseDatabase(
    override val pos: SqlParserPos,
    val database: SqlIdentifier?,
) : BitlapSqlDdlNode(pos, OPERATOR, listOfNotNull(database)) {

    companion object {
        val OPERATOR = SqlSpecialOperator("USE", SqlKind.OTHER)
    }

    override val resultTypes: List<Pair<String, SqlTypeName>>
        get() = listOf(
            "result" to SqlTypeName.BOOLEAN
        )

    override fun operator(sessionId: SessionId, context: DataContext): List<Array<Any?>> {
        if (database == null || database.simple == null) {
            throw BitlapException("Unable to use database with null.")
        }

        return listOf(
            arrayOf(catalog.useDatabase(sessionId, database.simple!!))
        )
    }
}

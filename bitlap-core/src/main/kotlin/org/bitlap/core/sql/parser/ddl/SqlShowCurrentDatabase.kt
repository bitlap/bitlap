/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.sql.parser.ddl

import org.apache.calcite.DataContext
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlTypeName
import org.bitlap.core.sql.QueryContext
import org.bitlap.core.sql.parser.BitlapSqlDdlNode

/**
 *  Parse tree for `SHOW current_database` statement.
 */
class SqlShowCurrentDatabase(
    override val pos: SqlParserPos
) : BitlapSqlDdlNode(pos, OPERATOR, emptyList()) {

    companion object {
        val OPERATOR = SqlSpecialOperator("SHOW CURRENT_DATABASE", SqlKind.OTHER)
    }

    override val resultTypes: List<Pair<String, SqlTypeName>>
        get() = listOf(
            "database_name" to SqlTypeName.VARCHAR
        )

    override fun operator(context: DataContext): List<Array<Any?>> {
        return listOf(
            arrayOf(QueryContext.get().currentSchema!!) // must exist
        )
    }
}

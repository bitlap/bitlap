/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.sql.parser.ddl

import org.apache.calcite.DataContext
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlTypeName
import org.bitlap.common.exception.BitlapException
import org.bitlap.core.sql.parser.BitlapSqlDdlNode

/**
 *    Parse tree for `USE database` statement.
 */
class SqlUseDatabase(
    override val pos: SqlParserPos,
    val database: SqlIdentifier?,
) : BitlapSqlDdlNode(pos, OPERATOR, listOfNotNull(database)) {

    lateinit var useDatabase: String

    companion object {
        val OPERATOR = SqlSpecialOperator("USE", SqlKind.OTHER)
    }

    override val resultTypes: List<Pair<String, SqlTypeName>>
        get() = listOf(
            "result" to SqlTypeName.VARCHAR
        )

    override fun operator(context: DataContext): List<Array<Any?>> {
        if (database == null || database.simple == null) {
            throw BitlapException("Unable to use database with null.")
        }
        val db = catalog.getDatabase(database.simple)
        this.useDatabase = db.name
        return listOf(arrayOf(db.name))
    }
}

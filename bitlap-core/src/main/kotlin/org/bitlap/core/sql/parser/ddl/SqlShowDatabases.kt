/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.sql.parser.ddl

import org.apache.calcite.DataContext
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlTypeName
import org.bitlap.core.sql.parser.BitlapSqlDdlNode

/**
 * Desc:
 *    Parse tree for `SHOW (DATABASES | SCHEMAS)` statement.
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/8/25
 */
class SqlShowDatabases(pos: SqlParserPos) : BitlapSqlDdlNode(pos, OPERATOR, emptyList()) {

    companion object {
        val OPERATOR = SqlSpecialOperator("SHOW DATABASES", SqlKind.OTHER)
    }

    override fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        writer.keyword("SHOW DATABASES")
    }

    override val resultTypes: List<Pair<String, SqlTypeName>>
        get() = listOf(
            "database_name" to SqlTypeName.VARCHAR
        )

    override fun operator(context: DataContext): List<Array<Any?>> {
        return catalog.listDatabases().map { arrayOf(it.name) }
    }
}

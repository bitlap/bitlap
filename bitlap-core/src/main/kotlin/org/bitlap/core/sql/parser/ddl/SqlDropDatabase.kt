/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.sql.parser.ddl

import org.apache.calcite.DataContext
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlTypeName
import org.bitlap.core.sql.parser.BitlapSqlDdlDropNode

/**
 * Desc:
 *   Parse tree for `DROP (DATABASE | SCHEMA) IF EXISTS database_name` statement.
 *
 * see [org.apache.calcite.sql.ddl.SqlDropSchema], you can also use [org.apache.calcite.sql.ddl.SqlDdlNodes]
 *
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/8/25
 */
class SqlDropDatabase(
    override val pos: SqlParserPos,
    val name: SqlIdentifier,
    override val ifExists: Boolean,
    val cascade: Boolean,
) : BitlapSqlDdlDropNode(
    pos, OPERATOR,
    listOf(SqlLiteral.createBoolean(ifExists, pos), name, SqlLiteral.createBoolean(cascade, pos)),
    ifExists
) {

    companion object {
        val OPERATOR = SqlSpecialOperator("DROP DATABASE", SqlKind.DROP_SCHEMA)
    }

    override fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        writer.keyword("DROP DATABASE")
        if (ifExists) {
            writer.keyword("IF EXISTS")
        }
        name.unparse(writer, leftPrec, rightPrec)
    }

    override val resultTypes: List<Pair<String, SqlTypeName>>
        get() = listOf(
            "result" to SqlTypeName.BOOLEAN
        )

    override fun operator(context: DataContext): List<Array<Any?>> {
        return listOf(
            arrayOf(catalog.dropDatabase(name.simple, ifExists, cascade))
        )
    }
}

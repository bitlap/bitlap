/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.sql.parser.ddl

import org.apache.calcite.DataContext
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlTypeName
import org.bitlap.core.sql.parser.BitlapSqlDdlCreateNode

/**
 * Desc:
 *   Parse tree for `CREATE (DATABASE | SCHEMA) IF NOT EXISTS database_name` statement.
 *
 * see [org.apache.calcite.sql.ddl.SqlCreateSchema], you can also use [org.apache.calcite.sql.ddl.SqlDdlNodes]
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/8/23
 */
class SqlCreateDatabase(
    override val pos: SqlParserPos,
    val name: SqlIdentifier,
    override val ifNotExists: Boolean,
    override val _replace: Boolean = false,
) : BitlapSqlDdlCreateNode(
    pos, OPERATOR,
    listOf(SqlLiteral.createBoolean(_replace, pos), SqlLiteral.createBoolean(ifNotExists, pos), name),
    _replace, ifNotExists
) {

    companion object {
        val OPERATOR = SqlSpecialOperator("CREATE DATABASE", SqlKind.CREATE_SCHEMA)
    }

    override fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        writer.keyword("CREATE DATABASE")
        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS")
        }
        name.unparse(writer, leftPrec, rightPrec)
    }

    override val resultTypes: List<Pair<String, SqlTypeName>>
        get() = listOf(
            "result" to SqlTypeName.BOOLEAN
        )

    override fun operator(context: DataContext): List<Array<Any?>> {
        return listOf(
            arrayOf(catalog.createDatabase(name.simple, ifNotExists))
        )
    }
}

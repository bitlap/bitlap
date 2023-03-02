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
 *   Parse tree for `CREATE (TABLE | DATASOURCE) IF NOT EXISTS table_name` statement.
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/8/23
 */
class SqlCreateTable(
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
        val OPERATOR = SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE)
    }

    override fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        writer.keyword("CREATE TABLE")
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
        val splits = name.names
        val result = if (splits.size == 1) {
            catalog.createTable(splits[0], ifNotExists = ifNotExists)
        } else {
            catalog.createTable(splits[1], splits[0], ifNotExists)
        }
        return listOf(arrayOf(result))
    }
}

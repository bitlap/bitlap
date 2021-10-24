package org.bitlap.core.sql.parser.ddl

import org.apache.calcite.DataContext
import org.apache.calcite.sql.*
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlTypeName
import org.bitlap.core.sql.parser.BitlapSqlDdlDropNode

/**
 * Desc:
 *   Parse tree for `DROP (TABLE | DATASOURCE) IF EXISTS table_name` statement.
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/8/25
 */
class SqlDropTable(
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
        val OPERATOR = SqlSpecialOperator("DROP TABLE", SqlKind.DROP_TABLE)
    }

    override val resultTypes: List<Pair<String, SqlTypeName>>
        get() = listOf(
            "result" to SqlTypeName.BOOLEAN
        )

    override fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        writer.keyword("DROP TABLE")
        if (ifExists) {
            writer.keyword("IF EXISTS")
        }
        name.unparse(writer, leftPrec, rightPrec)
    }

    override fun operator(context: DataContext): List<Array<Any?>> {
        val splits = name.names
        val result = if (splits.size == 1) {
            catalog.dropTable(splits[0], ifExists = ifExists, cascade = cascade)
        } else {
            catalog.dropTable(splits[1], splits[0], ifExists, cascade)
        }
        return listOf(arrayOf(result))
    }
}

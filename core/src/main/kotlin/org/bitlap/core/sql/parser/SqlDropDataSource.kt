package org.bitlap.core.sql.parser

import org.apache.calcite.sql.SqlDrop
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.parser.SqlParserPos

/**
 * Desc:
 *   Parse tree for `DROP (DATASOURCE | TABLE) IF EXISTS datasource_name` statement.
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/8/25
 */
class SqlDropDataSource(
    val pos: SqlParserPos,
    val name: SqlIdentifier,
    val ifExists: Boolean,
) : SqlDrop(OPERATOR, pos, ifExists) {

    companion object {
        val OPERATOR = SqlSpecialOperator("DROP TABLE", SqlKind.DROP_TABLE)
    }

    override fun getOperandList(): List<SqlNode> = listOf(name)

    override fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        writer.keyword("DROP TABLE")
        if (ifExists) {
            writer.keyword("IF EXISTS")
        }
        name.unparse(writer, leftPrec, rightPrec)
    }
}

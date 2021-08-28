package org.bitlap.core.sql.parser

import org.apache.calcite.sql.SqlCreate
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.parser.SqlParserPos

/**
 * Desc:
 *   Parse tree for `CREATE (DATASOURCE | TABLE) IF NOT EXISTS datasource_name` statement.
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/8/23
 */
class SqlCreateDataSource(
    val pos: SqlParserPos,
    val name: SqlIdentifier,
    val ifNotExists: Boolean,
    replace: Boolean = false,
) : SqlCreate(OPERATOR, pos, replace, ifNotExists) {

    companion object {
        val OPERATOR = SqlSpecialOperator("CREATE DATASOURCE", SqlKind.CREATE_TABLE)
    }

    override fun getOperandList(): List<SqlNode> = listOf(name)

    override fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        writer.keyword("CREATE DATASOURCE")
        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS")
        }
        name.unparse(writer, leftPrec, rightPrec)
    }
}

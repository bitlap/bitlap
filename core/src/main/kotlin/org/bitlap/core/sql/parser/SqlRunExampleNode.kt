package org.bitlap.core.sql.parser

import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.parser.SqlParserPos

/**
 * Desc:
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/7/28
 */
class SqlRunExampleNode(val pos: SqlParserPos, val exampleString: String) : SqlCall(pos) {

    companion object {
        val OPERATOR = SqlSpecialOperator("RUN EXAMPLE", SqlKind.OTHER)
    }

    override fun getOperator(): SqlOperator = OPERATOR
    override fun getOperandList(): List<SqlNode> = listOf(SqlLiteral.createCharString(exampleString, pos))
}

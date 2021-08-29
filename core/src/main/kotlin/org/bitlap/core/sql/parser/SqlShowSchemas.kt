package org.bitlap.core.sql.parser

import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.parser.SqlParserPos
import org.bitlap.core.data.BitlapCatalog

/**
 * Desc:
 *    Parse tree for `SHOW (SCHEMAS | DATABASES)` statement.
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/8/25
 */
class SqlShowSchemas(pos: SqlParserPos) : SqlCall(pos), SqlCommand {

    companion object {
        val OPERATOR = SqlSpecialOperator("SHOW SCHEMAS", SqlKind.OTHER)
    }

    override fun getOperator(): SqlOperator = OPERATOR

    override fun getOperandList(): List<SqlNode> = emptyList()

    override fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        writer.keyword("SHOW SCHEMAS")
    }

    override fun run(catalog: BitlapCatalog) {
        catalog.listSchemas()
    }
}

package org.bitlap.core.sql.parser.cmd

import org.apache.calcite.DataContext
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlTypeName
import org.bitlap.core.sql.parser.BitlapSqlDdlNode

/**
 * Desc: run example, this is an example, will be removed in future versions.
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/7/28
 */
class SqlRunExample(
    override val pos: SqlParserPos,
    private val exampleString: String,
) : BitlapSqlDdlNode(pos, OPERATOR, listOf(SqlLiteral.createCharString(exampleString, pos))) {

    companion object {
        val OPERATOR = SqlSpecialOperator("RUN EXAMPLE", SqlKind.OTHER)
    }

    override val resultTypes: List<Pair<String, SqlTypeName>>
        get() = listOf(
            "result" to SqlTypeName.VARCHAR,
        )

    override fun operator(context: DataContext): List<Array<Any?>> {
        return listOf(arrayOf("hello $exampleString"))
    }
}

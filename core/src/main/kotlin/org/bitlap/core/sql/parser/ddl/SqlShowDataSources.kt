package org.bitlap.core.sql.parser.ddl

import org.apache.calcite.DataContext
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlTypeName
import org.bitlap.core.sql.parser.BitlapSqlDdlNode

/**
 * Desc:
 *    Parse tree for `SHOW (DATASOURCES | TABLES) [IN schema_name]` statement.
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/8/25
 */
class SqlShowDataSources(
    override val pos: SqlParserPos,
    val schema: SqlIdentifier?,
) : BitlapSqlDdlNode(pos, OPERATOR, listOfNotNull(schema)) {

    companion object {
        val OPERATOR = SqlSpecialOperator("SHOW DATASOURCES", SqlKind.OTHER)
    }

    override val resultTypes: List<Pair<String, SqlTypeName>>
        get() = listOf(
            "table_name" to SqlTypeName.VARCHAR
        )

    override fun operator(context: DataContext): List<Array<Any?>> {
        if (schema == null) {
            return listOf(arrayOf("a"), arrayOf("b"), arrayOf("c"))
        } else {
            return listOf(arrayOf(schema))
        }
    }
}

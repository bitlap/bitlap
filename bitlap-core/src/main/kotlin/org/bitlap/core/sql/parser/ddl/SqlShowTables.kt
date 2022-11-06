/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.sql.parser.ddl

import org.apache.calcite.DataContext
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlTypeName
import org.bitlap.core.BitlapContext
import org.bitlap.core.sql.parser.BitlapSqlDdlNode

/**
 * Desc:
 *    Parse tree for `SHOW (TABLES | DATASOURCES) [IN schema_name]` statement.
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/8/25
 */
class SqlShowTables(
    override val pos: SqlParserPos,
    val database: SqlIdentifier?,
) : BitlapSqlDdlNode(pos, OPERATOR, listOfNotNull(database)) {

    companion object {
        val OPERATOR = SqlSpecialOperator("SHOW TABLES", SqlKind.OTHER)
    }

    override val resultTypes: List<Pair<String, SqlTypeName>>
        get() = listOf(
            "database_name" to SqlTypeName.VARCHAR,
            "table_name" to SqlTypeName.VARCHAR,
            "create_timestamp" to SqlTypeName.TIMESTAMP
        )

    override fun operator(context: DataContext): List<Array<Any?>> {
        return catalog.listTables(catalog.getCurrentDatabase()).map { arrayOf(it.database, it.name, it.createTime) }
    }
}

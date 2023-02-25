/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.sql.parser.cmd

import org.apache.calcite.DataContext
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.hadoop.fs.Path
import org.bitlap.core.BitlapContext
import org.bitlap.core.mdm.BitlapWriter
import org.bitlap.core.sql.parser.BitlapSqlDdlNode

/**
 * Desc: load data into table
 */
class SqlLoadData(
    override val pos: SqlParserPos,
    private val filePath: SqlNode,
    private val tableName: SqlIdentifier,
    private val overwrite: Boolean,
) : BitlapSqlDdlNode(pos, OPERATOR, listOf(filePath, tableName)) {

    companion object {
        val OPERATOR = SqlSpecialOperator("LOAD DATA", SqlKind.OTHER)
    }

    override val resultTypes: List<Pair<String, SqlTypeName>>
        get() = listOf(
            "result" to SqlTypeName.VARCHAR,
        )

    override fun operator(context: DataContext): List<Array<Any?>> {
        val path = filePath.toString().trim('\'')
        val table = if (tableName.names.size == 1) {
            catalog.getTable(tableName.simple)
        } else {
            catalog.getTable(tableName.names[1], tableName.names[0])
        }
        // only support overwrite = true
        BitlapWriter(table, BitlapContext.hadoopConf).use {
            if (path.startsWith("classpath:", true)) {
                it.writeCsv(path)
            }
            // hadoop file system should support protocal
            else {
                val p = Path(path)
                val fs = p.getFileSystem(BitlapContext.hadoopConf)
                it.writeCsv(fs.open(p))
            }
        }
        return listOf(arrayOf(true))
    }
}

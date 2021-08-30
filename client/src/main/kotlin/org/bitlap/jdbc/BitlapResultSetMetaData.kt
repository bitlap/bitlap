package org.bitlap.jdbc

import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.Types

/**
 *
 * @author 梦境迷离
 * @since 2021/6/12
 * @version 1.0
 */
open class BitlapResultSetMetaData(
    private val columnNames: List<String>,
    private val columnTypes: List<String>
) : ResultSetMetaData {

    override fun <T : Any?> unwrap(iface: Class<T>?): T {
        TODO("Not yet implemented")
    }

    override fun isWrapperFor(iface: Class<*>?): Boolean {
        TODO("Not yet implemented")
    }

    override fun getColumnCount(): Int {
        return columnNames.size
    }

    override fun isAutoIncrement(column: Int): Boolean {
        return false
    }

    override fun isCaseSensitive(column: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun isSearchable(column: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun isCurrency(column: Int): Boolean {
        return false
    }

    override fun isNullable(column: Int): Int {
        return ResultSetMetaData.columnNullable
    }

    override fun isSigned(column: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun getColumnDisplaySize(column: Int): Int {
        // taking a stab at appropriate values
        return when (getColumnType(column)) {
            Types.VARCHAR, Types.BIGINT -> 32
            Types.TINYINT -> 2
            Types.BOOLEAN -> 8
            Types.DOUBLE, Types.INTEGER -> 16
            else -> 32
        }
    }

    override fun getColumnLabel(column: Int): String {
        return columnNames[column - 1]
    }

    override fun getColumnName(column: Int): String {
        return columnNames[column - 1]
    }

    override fun getSchemaName(column: Int): String {
        TODO("Not yet implemented")
    }

    override fun getPrecision(column: Int): Int {
        return if (Types.DOUBLE == getColumnType(column)) -1 else 0 // Do we have a precision limit?
    }

    override fun getScale(column: Int): Int {
        return if (Types.DOUBLE == getColumnType(column)) -1 else 0 // Do we have a scale limit?
    }

    override fun getTableName(column: Int): String {
        TODO("Not yet implemented")
    }

    override fun getCatalogName(column: Int): String {
        TODO("Not yet implemented")
    }

    override fun getColumnType(column: Int): Int {
        if (columnTypes.isEmpty()) throw SQLException("Could not determine column type name for ResultSet")
        if (column < 1 || column > columnTypes.size) throw SQLException("Invalid column value: $column")
        return when (val type = columnTypes[column - 1]) {
            "string" -> Types.VARCHAR
            "bool" -> Types.BOOLEAN
            "double" -> Types.DOUBLE
            "byte" -> Types.TINYINT
            "i32" -> Types.INTEGER
            "i64" -> Types.BIGINT
            else -> throw SQLException("Inrecognized column type: $type")
        }
    }

    override fun getColumnTypeName(column: Int): String {
        TODO("Not yet implemented")
    }

    override fun isReadOnly(column: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun isWritable(column: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun isDefinitelyWritable(column: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun getColumnClassName(column: Int): String {
        TODO("Not yet implemented")
    }
}

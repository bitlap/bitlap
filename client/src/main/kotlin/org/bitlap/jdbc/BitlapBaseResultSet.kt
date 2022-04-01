/* Copyright (c) 2022 bitlap.org */
package org.bitlap.jdbc

import org.apache.commons.lang.StringUtils
import org.bitlap.network.BSQLException
import org.bitlap.network.proto.driver.BRow
import org.bitlap.network.proto.driver.BTableSchema
import org.bitlap.network.proto.driver.BTypeId
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.Blob
import java.sql.Clob
import java.sql.Date
import java.sql.NClob
import java.sql.Ref
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.RowId
import java.sql.SQLWarning
import java.sql.SQLXML
import java.sql.Statement
import java.sql.Time
import java.sql.Timestamp
import java.time.Instant
import java.util.Calendar

/**
 *
 * @author 梦境迷离
 * @since 2021/8/23
 * @version 1.0
 */
abstract class BitlapBaseResultSet : ResultSet {

    @JvmField
    protected var warningChain: SQLWarning? = null

    protected open var row: BRow? = null

    protected open val columnNames: MutableList<String> by lazy { mutableListOf() }

    protected open val columnTypes: MutableList<String> by lazy { mutableListOf() }

    private lateinit var schema: BTableSchema

    private var wasNull: Boolean = false

    override fun <T : Any?> unwrap(iface: Class<T>?): T {
        TODO("Not yet implemented")
    }

    override fun isWrapperFor(iface: Class<*>?): Boolean {
        TODO("Not yet implemented")
    }

    override fun close() {
        TODO("Not yet implemented")
    }

    override fun wasNull(): Boolean {
        return wasNull
    }

    override fun getString(columnIndex: Int): String {
        return getColumnValue(columnIndex)
    }

    override fun getString(columnLabel: String?): String {
        return getString(findColumn(columnLabel))
    }

    override fun getBoolean(columnIndex: Int): Boolean {
        return getColumnValue(columnIndex)
    }

    override fun getBoolean(columnLabel: String?): Boolean {
        return getBoolean(findColumn(columnLabel))
    }

    override fun getByte(columnIndex: Int): Byte {
        TODO("Not yet implemented")
    }

    override fun getByte(columnLabel: String?): Byte {
        TODO("Not yet implemented")
    }

    override fun getShort(columnIndex: Int): Short {
        return getColumnValue(columnIndex)
    }

    override fun getShort(columnLabel: String?): Short {
        return getShort(findColumn(columnLabel))
    }

    override fun getInt(columnIndex: Int): Int {
        return getColumnValue(columnIndex)
    }

    override fun getInt(columnLabel: String?): Int {
        return getInt(findColumn(columnLabel))
    }

    override fun getLong(columnIndex: Int): Long {
        return getColumnValue(columnIndex)
    }

    override fun getLong(columnLabel: String?): Long {
        return getLong(findColumn(columnLabel))
    }

    override fun getFloat(columnIndex: Int): Float {
        TODO("Not yet implemented")
    }

    override fun getFloat(columnLabel: String?): Float {
        TODO("Not yet implemented")
    }

    override fun getDouble(columnIndex: Int): Double {
        return getColumnValue(columnIndex)
    }

    override fun getDouble(columnLabel: String?): Double {
        return getDouble(findColumn(columnLabel))
    }

    override fun getBigDecimal(columnIndex: Int, scale: Int): BigDecimal {
        TODO("Not yet implemented")
    }

    override fun getBigDecimal(columnLabel: String?, scale: Int): BigDecimal {
        TODO("Not yet implemented")
    }

    override fun getBigDecimal(columnIndex: Int): BigDecimal {
        TODO("Not yet implemented")
    }

    override fun getBigDecimal(columnLabel: String?): BigDecimal {
        TODO("Not yet implemented")
    }

    override fun getBytes(columnIndex: Int): ByteArray {
        TODO("Not yet implemented")
    }

    override fun getBytes(columnLabel: String?): ByteArray {
        TODO("Not yet implemented")
    }

    override fun getDate(columnIndex: Int): Date {
        TODO("Not yet implemented")
    }

    override fun getDate(columnLabel: String?): Date {
        TODO("Not yet implemented")
    }

    override fun getDate(columnIndex: Int, cal: Calendar?): Date {
        TODO("Not yet implemented")
    }

    override fun getDate(columnLabel: String?, cal: Calendar?): Date {
        TODO("Not yet implemented")
    }

    override fun getTime(columnIndex: Int): Time {
        TODO("Not yet implemented")
    }

    override fun getTime(columnLabel: String?): Time {
        TODO("Not yet implemented")
    }

    override fun getTime(columnIndex: Int, cal: Calendar?): Time {
        TODO("Not yet implemented")
    }

    override fun getTime(columnLabel: String?, cal: Calendar?): Time {
        TODO("Not yet implemented")
    }

    override fun getTimestamp(columnIndex: Int): Timestamp {
        return getColumnValue(columnIndex)
    }

    override fun getTimestamp(columnLabel: String?): Timestamp {
        return getTimestamp(findColumn(columnLabel))
    }

    override fun getTimestamp(columnIndex: Int, cal: Calendar?): Timestamp {
        TODO("Not yet implemented")
    }

    override fun getTimestamp(columnLabel: String?, cal: Calendar?): Timestamp {
        TODO("Not yet implemented")
    }

    override fun getAsciiStream(columnIndex: Int): InputStream {
        TODO("Not yet implemented")
    }

    override fun getAsciiStream(columnLabel: String?): InputStream {
        TODO("Not yet implemented")
    }

    override fun getUnicodeStream(columnIndex: Int): InputStream {
        TODO("Not yet implemented")
    }

    override fun getUnicodeStream(columnLabel: String?): InputStream {
        TODO("Not yet implemented")
    }

    override fun getBinaryStream(columnIndex: Int): InputStream {
        TODO("Not yet implemented")
    }

    override fun getBinaryStream(columnLabel: String?): InputStream {
        TODO("Not yet implemented")
    }

    override fun getWarnings(): SQLWarning? {
        return warningChain
    }

    override fun clearWarnings() {
        warningChain = null
    }

    override fun getCursorName(): String {
        TODO("Not yet implemented")
    }

    override fun getMetaData(): ResultSetMetaData {
        return BitlapResultSetMetaData(columnNames, columnTypes)
    }

    override fun getObject(columnIndex: Int): Any {
        TODO("Not yet implemented")
    }

    override fun getObject(columnLabel: String?): Any {
        TODO("Not yet implemented")
    }

    override fun getObject(columnIndex: Int, map: MutableMap<String, Class<*>>?): Any {
        TODO("Not yet implemented")
    }

    override fun getObject(columnLabel: String?, map: MutableMap<String, Class<*>>?): Any {
        TODO("Not yet implemented")
    }

    override fun <T : Any?> getObject(columnIndex: Int, type: Class<T>?): T {
        TODO("Not yet implemented")
    }

    override fun <T : Any?> getObject(columnLabel: String?, type: Class<T>?): T {
        TODO("Not yet implemented")
    }

    override fun findColumn(columnLabel: String?): Int {
        var columnIndex = columnNames.indexOf(columnLabel)
        return if (columnIndex == -1) {
            throw BSQLException("Bitlap SQL Exception", null)
        } else {
            ++columnIndex
        }
    }

    override fun getCharacterStream(columnIndex: Int): Reader {
        TODO("Not yet implemented")
    }

    override fun getCharacterStream(columnLabel: String?): Reader {
        TODO("Not yet implemented")
    }

    override fun isBeforeFirst(): Boolean {
        TODO("Not yet implemented")
    }

    override fun isAfterLast(): Boolean {
        TODO("Not yet implemented")
    }

    override fun isFirst(): Boolean {
        TODO("Not yet implemented")
    }

    override fun isLast(): Boolean {
        TODO("Not yet implemented")
    }

    override fun beforeFirst() {
        TODO("Not yet implemented")
    }

    override fun afterLast() {
        TODO("Not yet implemented")
    }

    override fun first(): Boolean {
        TODO("Not yet implemented")
    }

    override fun last(): Boolean {
        TODO("Not yet implemented")
    }

    override fun getRow(): Int {
        TODO("Not yet implemented")
    }

    override fun absolute(row: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun relative(rows: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun previous(): Boolean {
        TODO("Not yet implemented")
    }

    override fun setFetchDirection(direction: Int) {
        TODO("Not yet implemented")
    }

    override fun getFetchDirection(): Int {
        TODO("Not yet implemented")
    }

    override fun setFetchSize(rows: Int) {
        TODO("Not yet implemented")
    }

    override fun getFetchSize(): Int {
        TODO("Not yet implemented")
    }

    override fun getType(): Int {
        return ResultSet.TYPE_FORWARD_ONLY
    }

    override fun getConcurrency(): Int {
        TODO("Not yet implemented")
    }

    override fun rowUpdated(): Boolean {
        TODO("Not yet implemented")
    }

    override fun rowInserted(): Boolean {
        TODO("Not yet implemented")
    }

    override fun rowDeleted(): Boolean {
        TODO("Not yet implemented")
    }

    override fun updateNull(columnIndex: Int) {
        TODO("Not yet implemented")
    }

    override fun updateNull(columnLabel: String?) {
        TODO("Not yet implemented")
    }

    override fun updateBoolean(columnIndex: Int, x: Boolean) {
        TODO("Not yet implemented")
    }

    override fun updateBoolean(columnLabel: String?, x: Boolean) {
        TODO("Not yet implemented")
    }

    override fun updateByte(columnIndex: Int, x: Byte) {
        TODO("Not yet implemented")
    }

    override fun updateByte(columnLabel: String?, x: Byte) {
        TODO("Not yet implemented")
    }

    override fun updateShort(columnIndex: Int, x: Short) {
        TODO("Not yet implemented")
    }

    override fun updateShort(columnLabel: String?, x: Short) {
        TODO("Not yet implemented")
    }

    override fun updateInt(columnIndex: Int, x: Int) {
        TODO("Not yet implemented")
    }

    override fun updateInt(columnLabel: String?, x: Int) {
        TODO("Not yet implemented")
    }

    override fun updateLong(columnIndex: Int, x: Long) {
        TODO("Not yet implemented")
    }

    override fun updateLong(columnLabel: String?, x: Long) {
        TODO("Not yet implemented")
    }

    override fun updateFloat(columnIndex: Int, x: Float) {
        TODO("Not yet implemented")
    }

    override fun updateFloat(columnLabel: String?, x: Float) {
        TODO("Not yet implemented")
    }

    override fun updateDouble(columnIndex: Int, x: Double) {
        TODO("Not yet implemented")
    }

    override fun updateDouble(columnLabel: String?, x: Double) {
        TODO("Not yet implemented")
    }

    override fun updateBigDecimal(columnIndex: Int, x: BigDecimal?) {
        TODO("Not yet implemented")
    }

    override fun updateBigDecimal(columnLabel: String?, x: BigDecimal?) {
        TODO("Not yet implemented")
    }

    override fun updateString(columnIndex: Int, x: String?) {
        TODO("Not yet implemented")
    }

    override fun updateString(columnLabel: String?, x: String?) {
        TODO("Not yet implemented")
    }

    override fun updateBytes(columnIndex: Int, x: ByteArray?) {
        TODO("Not yet implemented")
    }

    override fun updateBytes(columnLabel: String?, x: ByteArray?) {
        TODO("Not yet implemented")
    }

    override fun updateDate(columnIndex: Int, x: Date?) {
        TODO("Not yet implemented")
    }

    override fun updateDate(columnLabel: String?, x: Date?) {
        TODO("Not yet implemented")
    }

    override fun updateTime(columnIndex: Int, x: Time?) {
        TODO("Not yet implemented")
    }

    override fun updateTime(columnLabel: String?, x: Time?) {
        TODO("Not yet implemented")
    }

    override fun updateTimestamp(columnIndex: Int, x: Timestamp?) {
        TODO("Not yet implemented")
    }

    override fun updateTimestamp(columnLabel: String?, x: Timestamp?) {
        TODO("Not yet implemented")
    }

    override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Int) {
        TODO("Not yet implemented")
    }

    override fun updateAsciiStream(columnLabel: String?, x: InputStream?, length: Int) {
        TODO("Not yet implemented")
    }

    override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateAsciiStream(columnLabel: String?, x: InputStream?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateAsciiStream(columnIndex: Int, x: InputStream?) {
        TODO("Not yet implemented")
    }

    override fun updateAsciiStream(columnLabel: String?, x: InputStream?) {
        TODO("Not yet implemented")
    }

    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Int) {
        TODO("Not yet implemented")
    }

    override fun updateBinaryStream(columnLabel: String?, x: InputStream?, length: Int) {
        TODO("Not yet implemented")
    }

    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateBinaryStream(columnLabel: String?, x: InputStream?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateBinaryStream(columnIndex: Int, x: InputStream?) {
        TODO("Not yet implemented")
    }

    override fun updateBinaryStream(columnLabel: String?, x: InputStream?) {
        TODO("Not yet implemented")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Int) {
        TODO("Not yet implemented")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Int) {
        TODO("Not yet implemented")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?) {
        TODO("Not yet implemented")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?) {
        TODO("Not yet implemented")
    }

    override fun updateObject(columnIndex: Int, x: Any?, scaleOrLength: Int) {
        TODO("Not yet implemented")
    }

    override fun updateObject(columnIndex: Int, x: Any?) {
        TODO("Not yet implemented")
    }

    override fun updateObject(columnLabel: String?, x: Any?, scaleOrLength: Int) {
        TODO("Not yet implemented")
    }

    override fun updateObject(columnLabel: String?, x: Any?) {
        TODO("Not yet implemented")
    }

    override fun insertRow() {
        TODO("Not yet implemented")
    }

    override fun updateRow() {
        TODO("Not yet implemented")
    }

    override fun deleteRow() {
        TODO("Not yet implemented")
    }

    override fun refreshRow() {
        TODO("Not yet implemented")
    }

    override fun cancelRowUpdates() {
        TODO("Not yet implemented")
    }

    override fun moveToInsertRow() {
        TODO("Not yet implemented")
    }

    override fun moveToCurrentRow() {
        TODO("Not yet implemented")
    }

    override fun getStatement(): Statement {
        TODO("Not yet implemented")
    }

    override fun getRef(columnIndex: Int): Ref {
        TODO("Not yet implemented")
    }

    override fun getRef(columnLabel: String?): Ref {
        TODO("Not yet implemented")
    }

    override fun getBlob(columnIndex: Int): Blob {
        TODO("Not yet implemented")
    }

    override fun getBlob(columnLabel: String?): Blob {
        TODO("Not yet implemented")
    }

    override fun getClob(columnIndex: Int): Clob {
        TODO("Not yet implemented")
    }

    override fun getClob(columnLabel: String?): Clob {
        TODO("Not yet implemented")
    }

    override fun getArray(columnIndex: Int): java.sql.Array {
        TODO("Not yet implemented")
    }

    override fun getArray(columnLabel: String?): java.sql.Array {
        TODO("Not yet implemented")
    }

    override fun getURL(columnIndex: Int): URL {
        TODO("Not yet implemented")
    }

    override fun getURL(columnLabel: String?): URL {
        TODO("Not yet implemented")
    }

    override fun updateRef(columnIndex: Int, x: Ref?) {
        TODO("Not yet implemented")
    }

    override fun updateRef(columnLabel: String?, x: Ref?) {
        TODO("Not yet implemented")
    }

    override fun updateBlob(columnIndex: Int, x: Blob?) {
        TODO("Not yet implemented")
    }

    override fun updateBlob(columnLabel: String?, x: Blob?) {
        TODO("Not yet implemented")
    }

    override fun updateBlob(columnIndex: Int, inputStream: InputStream?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateBlob(columnLabel: String?, inputStream: InputStream?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateBlob(columnIndex: Int, inputStream: InputStream?) {
        TODO("Not yet implemented")
    }

    override fun updateBlob(columnLabel: String?, inputStream: InputStream?) {
        TODO("Not yet implemented")
    }

    override fun updateClob(columnIndex: Int, x: Clob?) {
        TODO("Not yet implemented")
    }

    override fun updateClob(columnLabel: String?, x: Clob?) {
        TODO("Not yet implemented")
    }

    override fun updateClob(columnIndex: Int, reader: Reader?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateClob(columnLabel: String?, reader: Reader?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateClob(columnIndex: Int, reader: Reader?) {
        TODO("Not yet implemented")
    }

    override fun updateClob(columnLabel: String?, reader: Reader?) {
        TODO("Not yet implemented")
    }

    override fun updateArray(columnIndex: Int, x: java.sql.Array?) {
        TODO("Not yet implemented")
    }

    override fun updateArray(columnLabel: String?, x: java.sql.Array?) {
        TODO("Not yet implemented")
    }

    override fun getRowId(columnIndex: Int): RowId {
        TODO("Not yet implemented")
    }

    override fun getRowId(columnLabel: String?): RowId {
        TODO("Not yet implemented")
    }

    override fun updateRowId(columnIndex: Int, x: RowId?) {
        TODO("Not yet implemented")
    }

    override fun updateRowId(columnLabel: String?, x: RowId?) {
        TODO("Not yet implemented")
    }

    override fun getHoldability(): Int {
        TODO("Not yet implemented")
    }

    override fun isClosed(): Boolean {
        TODO("Not yet implemented")
    }

    override fun updateNString(columnIndex: Int, nString: String?) {
        TODO("Not yet implemented")
    }

    override fun updateNString(columnLabel: String?, nString: String?) {
        TODO("Not yet implemented")
    }

    override fun updateNClob(columnIndex: Int, nClob: NClob?) {
        TODO("Not yet implemented")
    }

    override fun updateNClob(columnLabel: String?, nClob: NClob?) {
        TODO("Not yet implemented")
    }

    override fun updateNClob(columnIndex: Int, reader: Reader?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateNClob(columnLabel: String?, reader: Reader?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateNClob(columnIndex: Int, reader: Reader?) {
        TODO("Not yet implemented")
    }

    override fun updateNClob(columnLabel: String?, reader: Reader?) {
        TODO("Not yet implemented")
    }

    override fun getNClob(columnIndex: Int): NClob {
        TODO("Not yet implemented")
    }

    override fun getNClob(columnLabel: String?): NClob {
        TODO("Not yet implemented")
    }

    override fun getSQLXML(columnIndex: Int): SQLXML {
        TODO("Not yet implemented")
    }

    override fun getSQLXML(columnLabel: String?): SQLXML {
        TODO("Not yet implemented")
    }

    override fun updateSQLXML(columnIndex: Int, xmlObject: SQLXML?) {
        TODO("Not yet implemented")
    }

    override fun updateSQLXML(columnLabel: String?, xmlObject: SQLXML?) {
        TODO("Not yet implemented")
    }

    override fun getNString(columnIndex: Int): String {
        TODO("Not yet implemented")
    }

    override fun getNString(columnLabel: String?): String {
        TODO("Not yet implemented")
    }

    override fun getNCharacterStream(columnIndex: Int): Reader {
        TODO("Not yet implemented")
    }

    override fun getNCharacterStream(columnLabel: String?): Reader {
        TODO("Not yet implemented")
    }

    override fun updateNCharacterStream(columnIndex: Int, x: Reader?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateNCharacterStream(columnLabel: String?, reader: Reader?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateNCharacterStream(columnIndex: Int, x: Reader?) {
        TODO("Not yet implemented")
    }

    override fun updateNCharacterStream(columnLabel: String?, reader: Reader?) {
        TODO("Not yet implemented")
    }

    @Suppress("UNCHECKED_CAST")
    private inline fun <reified T> getColumnValue(columnIndex: Int): T {
        if (row == null) {
            throw BSQLException("No row found.", null)
        }
        val colVals = row!!.colValsList ?: throw BSQLException("RowSet does not contain any columns!", null)
        if (columnIndex > colVals.size) {
            throw BSQLException("Invalid columnIndex: $columnIndex", null)
        }

        // In kotlin, We can not use e.g. Int?,Long?,Double? to override the java interface here.
        val bColumnValue = colVals[columnIndex - 1]
        try {
            if (bColumnValue.isEmpty) {
                wasNull = true
            }

            val columnType = getSchema().getColumns(columnIndex - 1).typeDesc
            return (
                when (columnType) {
                    BTypeId.B_TYPE_ID_STRING_TYPE ->
                        if (bColumnValue.toStringUtf8().isEmpty()) StringUtils.EMPTY else bColumnValue.toStringUtf8()
                    BTypeId.B_TYPE_ID_INT_TYPE ->
                        if (bColumnValue.toStringUtf8().isNotEmpty()) Integer.parseInt(bColumnValue.toStringUtf8()) else 0
                    BTypeId.B_TYPE_ID_DOUBLE_TYPE ->
                        if (bColumnValue.toStringUtf8()
                            .isNotEmpty()
                        ) java.lang.Double.parseDouble(bColumnValue.toStringUtf8()) else 0.0
                    BTypeId.B_TYPE_ID_SHORT_TYPE ->
                        if (bColumnValue.toStringUtf8()
                            .isNotEmpty()
                        ) java.lang.Short.parseShort(bColumnValue.toStringUtf8()) else 0
                    BTypeId.B_TYPE_ID_LONG_TYPE ->
                        if (bColumnValue.toStringUtf8()
                            .isNotEmpty()
                        ) java.lang.Long.parseLong(bColumnValue.toStringUtf8()) else 0
                    BTypeId.B_TYPE_ID_BOOLEAN_TYPE ->
                        if (bColumnValue.toStringUtf8()
                            .isNotEmpty()
                        ) java.lang.Boolean.valueOf(bColumnValue.toStringUtf8()) else false
                    BTypeId.B_TYPE_ID_TIMESTAMP_TYPE ->
                        if (bColumnValue.toStringUtf8()
                            .isNotEmpty()
                        ) Timestamp.from(Instant.ofEpochMilli(java.lang.Long.parseLong(bColumnValue.toStringUtf8()))) else Timestamp.from(Instant.now())
                    else -> throw BSQLException("Unrecognized column type:$columnType", null)
                }
                ) as T
        } catch (e: Exception) {
            e.printStackTrace()
            throw e
        }
    }

    fun setSchema(schema: BTableSchema) {
        this.schema = schema
    }

    fun getSchema(): BTableSchema {
        return this.schema
    }
}

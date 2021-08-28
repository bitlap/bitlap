package org.bitlap.jdbc

import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl
import org.bitlap.network.BSQLException
import org.bitlap.network.client.BitlapClient.fetchResults
import org.bitlap.network.client.BitlapClient.getResultSetMetadata
import org.bitlap.network.proto.driver.BOperationHandle
import org.bitlap.network.proto.driver.BRow
import org.bitlap.network.proto.driver.BSessionHandle
import java.sql.SQLException

/**
 *
 * @author 梦境迷离
 * @since 2021/6/12
 * @version 1.0
 */
open class BitlapQueryResultSet(
    private var client: CliClientServiceImpl?,
    private var maxRows: Int,
    override var row: BRow? = null
) : BitlapBaseResultSet() {

    private var emptyResultSet = false
    private var rowsFetched = 0

    @JvmField
    protected var isClosed = false

    @JvmField
    protected var fetchSize = 0

    private var fetchedRows: List<BRow> = listOf()
    private var fetchedRowsItr: Iterator<BRow> = fetchedRows.iterator()
    private var sessHandle: BSessionHandle? = null
    private var stmtHandle: BOperationHandle? = null

    constructor(builder: Builder) : this(builder.client, builder.maxRows) {
        this.client = builder.client
        this.stmtHandle = builder.stmtHandle
        this.sessHandle = builder.sessHandle
        this.fetchSize = builder.fetchSize
        this.columnNames = builder.colNames
        this.columnTypes = builder.colTypes
        if (builder.retrieveSchema) {
            retrieveSchema()
        } else {
            this.columnNames.addAll(builder.colNames)
            this.columnTypes.addAll(builder.colTypes)
        }
        this.emptyResultSet = builder.emptyResultSet
        maxRows = if (builder.emptyResultSet) {
            0
        } else {
            builder.maxRows
        }
    }

    private fun retrieveSchema() {
        try {
            if (client == null || stmtHandle == null) {
                throw BSQLException("Resultset is closed")
            }
            val namesSb = StringBuilder()
            val typesSb = StringBuilder()

            val schema = client?.getResultSetMetadata(stmtHandle!!)
            if (schema == null || schema.columnsList.isEmpty()) {
                return
            }

            this.setSchema(schema)
            val columns = schema.columnsList
            for (pos in 0 until schema.columnsCount) {
                if (pos != 0) {
                    namesSb.append(",")
                    typesSb.append(",")
                }
                val columnName = columns[pos].columnName
                columnNames.add(columnName)
                val columnTypeName = Utils.typeNames[columns[pos].typeDesc]!!
                columnTypes.add(columnTypeName)
                namesSb.append(columnName)
                typesSb.append(columnTypeName)
            }
            println("retrieveSchema => names: $namesSb, types: $typesSb")
        } catch (e: SQLException) {
            throw e
        } catch (e: Exception) {
            e.printStackTrace()
            throw SQLException("Could not create ResultSet: " + e.message, e)
        }
    }

    override fun next(): Boolean {
        if (isClosed || client === null || stmtHandle == null) {
            throw BSQLException("Resultset is closed")
        }
        if (emptyResultSet || maxRows in 1..rowsFetched) {
            return false
        }
        try {

            if (fetchedRows.isEmpty() || !fetchedRowsItr.hasNext()) {
                val result = client?.fetchResults(stmtHandle!!)
                if (result != null) {
                    fetchedRows = result.results?.rowsList.orEmpty()
                    fetchedRowsItr = fetchedRows.iterator()
                }
            }
            if (fetchedRowsItr.hasNext()) {
                row = fetchedRowsItr.next()
            } else {
                return false
            }

            rowsFetched++
        } catch (e: SQLException) {
            throw e
        } catch (e: Exception) {
            e.printStackTrace()
            throw SQLException("Error retrieving next row", e)
        }

        return true
    }

    override fun isClosed(): Boolean {
        return this.isClosed
    }

    override fun getFetchSize(): Int {
        return this.fetchSize
    }

    override fun setFetchSize(rows: Int) {
        this.fetchSize = rows
    }

    override fun close() {
        this.client = null
        this.stmtHandle = null
        this.sessHandle = null
        this.isClosed = true
    }

    companion object {
        fun builder(): Builder = Builder()
        class Builder {
            lateinit var client: CliClientServiceImpl
            lateinit var stmtHandle: BOperationHandle
            lateinit var sessHandle: BSessionHandle

            /**
             * Sets the limit for the maximum number of rows that any ResultSet object produced by this
             * Statement can contain to the given number. If the limit is exceeded, the excess rows
             * are silently dropped. The value must be >= 0, and 0 means there is not limit.
             */
            var maxRows = 0
            var retrieveSchema = true
            var colNames: MutableList<String> = mutableListOf()
            var colTypes: MutableList<String> = mutableListOf()
            var fetchSize = 50
            var emptyResultSet = false
            fun setClient(client: CliClientServiceImpl): Builder {
                this.client = client
                return this
            }

            fun setStmtHandle(stmtHandle: BOperationHandle): Builder {
                this.stmtHandle = stmtHandle
                return this
            }

            fun setSessionHandle(sessHandle: BSessionHandle): Builder {
                this.sessHandle = sessHandle
                return this
            }

            fun setMaxRows(maxRows: Int): Builder {
                this.maxRows = maxRows
                return this
            }

            fun setSchema(colNames: List<String>, colTypes: List<String>): Builder {
                this.colNames.addAll(colNames)
                this.colTypes.addAll(colTypes)
                retrieveSchema = false
                return this
            }

            fun setFetchSize(fetchSize: Int): Builder {
                this.fetchSize = fetchSize
                return this
            }

            fun setEmptyResultSet(emptyResultSet: Boolean): Builder {
                this.emptyResultSet = emptyResultSet
                return this
            }

            fun build(): BitlapQueryResultSet {
                return BitlapQueryResultSet(this)
            }
        }
    }
}

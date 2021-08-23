package org.bitlap.jdbc

import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl
import java.sql.SQLException
import org.bitlap.common.client.BitlapClient.fetchResults
import org.bitlap.common.proto.driver.BOperationHandle
import org.bitlap.common.proto.driver.BSessionHandle

/**
 *
 * @author 梦境迷离
 * @since 2021/6/12
 * @version 1.0
 */
class BitlapQueryResultSet(
    private var client: CliClientServiceImpl?,
    private var maxRows: Int,
    override var row: Array<*>? = arrayOfNulls<Any>(7)
) : BitlapBaseResultSet() {

    private var emptyResultSet = false
    private var rowsFetched = 0

    @JvmField
    protected var isClosed = false

    @JvmField
    protected var fetchSize = 0

    private var fetchedRows: List<String>? = null
    private var fetchedRowsItr: Iterator<String>? = null
    private var sessHandle: BSessionHandle? = null
    private var stmtHandle: BOperationHandle? = null
    // protected lateinit schema BTableSchema todo

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

    }

    override fun next(): Boolean {
        if (isClosed || client === null) {
            throw SQLException("Resultset is closed")
        }
        if (emptyResultSet || maxRows in 1..rowsFetched) {
            return false
        }
        if (fetchedRows == null || !fetchedRowsItr!!.hasNext()) {
            fetchedRows = stmtHandle?.let { client?.fetchResults(it) }.orEmpty()
            fetchedRowsItr = fetchedRows!!.iterator()
        }

        if (fetchedRowsItr!!.hasNext()) {
            row = arrayOf(fetchedRowsItr!!.next())
        } else {
            return false
        }

        rowsFetched++

        return true //TODO Moves the cursor down one row from its current position.
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

//    protected open fun setSchema(schema: BTableSchema) {
//        this.schema = schema
//    }
//
//    protected open fun getSchema(): BTableSchema? {
//        return schema
//    }

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

            fun setSchema(colNames: MutableList<String>?, colTypes: List<String>?): Builder {
                this.colNames.addAll(colNames!!)
                this.colTypes.addAll(colTypes!!)
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

            @Throws(SQLException::class)
            fun build(): BitlapQueryResultSet {
                return BitlapQueryResultSet(this)
            }
        }
    }
}

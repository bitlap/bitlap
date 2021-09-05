package org.bitlap.server.core

import org.bitlap.common.BitlapConf
import org.bitlap.core.sql.QueryExecution
import org.bitlap.core.sql.QueryResult
import org.bitlap.network.core.HandleIdentifier
import org.bitlap.network.core.RowSet
import org.bitlap.network.core.Session
import org.bitlap.network.core.SessionHandle
import org.bitlap.network.core.SessionManager
import org.bitlap.network.core.TableSchema
import org.bitlap.network.core.operation.OperationHandle
import org.bitlap.network.core.operation.OperationManager
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Bitlap Session
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
class BitlapSession() : Session {

    @Volatile
    override var lastAccessTime: Long = System.currentTimeMillis()
    override lateinit var username: String
    override lateinit var password: String
    override lateinit var sessionHandle: SessionHandle
    override lateinit var sessionConf: BitlapConf
    override val creationTime: Long = System.currentTimeMillis()
    override lateinit var sessionManager: SessionManager
    override lateinit var operationManager: OperationManager

    override val sessionState: AtomicBoolean = AtomicBoolean(false)

    private val cache: MutableMap<OperationHandle, QueryResult> = mutableMapOf() // TODO optimize by operationManager

    private val opHandleSet: MutableSet<OperationHandle> = mutableSetOf()

    constructor(
        username: String,
        password: String,
        sessionConf: Map<String, String>,
        sessionManager: SessionManager,
        sessionHandle: SessionHandle = SessionHandle(HandleIdentifier())
    ) : this() {
        this.username = username
        this.sessionHandle = sessionHandle
        this.password = password
        this.sessionConf = BitlapConf(sessionConf)
        this.sessionState.compareAndSet(false, true)
        this.sessionManager = sessionManager
    }

    override fun open(sessionConfMap: Map<String, String>?): SessionHandle {
        TODO("Not yet implemented")
    }

    override fun executeStatement(
        sessionHandle: SessionHandle,
        statement: String,
        confOverlay: Map<String, String>?
    ): OperationHandle {
        val operation = operationManager.newExecuteStatementOperation(this, statement, confOverlay)
        opHandleSet.add(operation.opHandle)
        cache[operation.opHandle] = QueryExecution(statement).execute()
        return operation.opHandle
    }

    override fun executeStatement(
        sessionHandle: SessionHandle,
        statement: String,
        confOverlay: Map<String, String>?,
        queryTimeout: Long
    ): OperationHandle {
        return executeStatement(sessionHandle, statement, confOverlay)
    }

    override fun fetchResults(operationHandle: OperationHandle): RowSet {
        val rows = cache[operationHandle]?.rows ?: RowSet()
        // TODO: remove cache
        cache.remove(operationHandle)
        return rows
    }

    override fun getResultSetMetadata(operationHandle: OperationHandle): TableSchema {
        return cache[operationHandle]?.tableSchema ?: TableSchema()
    }

    override fun close() {
        TODO("Not yet implemented")
    }
}

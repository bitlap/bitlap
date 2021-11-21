package org.bitlap.server.core

import org.bitlap.common.BitlapConf
import org.bitlap.network.core.RowSet
import org.bitlap.network.core.TableSchema
import org.bitlap.network.core.operation.OperationHandle
import org.bitlap.network.core.operation.OperationManager
import java.util.concurrent.atomic.AtomicBoolean
import org.bitlap.net.handles
import org.bitlap.net.operation.OperationManager
import org.bitlap.net.session.Session
import org.bitlap.net.session.SessionManager

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
    override lateinit var sessionHandle: handles.SessionHandle
    override lateinit var sessionConf: BitlapConf
    override val creationTime: Long = System.currentTimeMillis()
    override lateinit var sessionManager: SessionManager
    override lateinit var operationManager: OperationManager

    override val sessionState: AtomicBoolean = AtomicBoolean(false)
    private val opHandleSet: MutableSet<handles.OperationHandle> = mutableSetOf()

    constructor(
        username: String,
        password: String,
        sessionConf: Map<String, String>,
        sessionManager: SessionManager,
        sessionHandle: handles.SessionHandle = handles.SessionHandle(handles.HandleIdentifier())
    ) : this() {
        this.username = username
        this.sessionHandle = sessionHandle
        this.password = password
        this.sessionConf = BitlapConf(sessionConf)
        this.sessionState.compareAndSet(false, true)
        this.sessionManager = sessionManager
    }

    override fun open(sessionConfMap: Map<String, String>?): handles.SessionHandle {
        TODO("Not yet implemented")
    }

    override fun executeStatement(
        sessionHandle: handles.SessionHandle,
        statement: String,
        confOverlay: Map<String, String>?
    ): handles.OperationHandle {
        val operation = operationManager.newExecuteStatementOperation(this, statement, confOverlay)
        opHandleSet.add(operation.opHandle)
        return operation.opHandle
    }

    override fun executeStatement(
        sessionHandle: handles.SessionHandle,
        statement: String,
        confOverlay: Map<String, String>?,
        queryTimeout: Long
    ): handles.OperationHandle {
        return executeStatement(sessionHandle, statement, confOverlay)
    }

    override fun fetchResults(operationHandle: handles.OperationHandle): RowSet {
        val op = operationManager.getOperation(operationHandle)
        val rows = op.getNextResultSet()
        op.remove(operationHandle) // TODO: work with fetch offset & size
        return rows
    }

    override fun getResultSetMetadata(operationHandle: handles.OperationHandle): TableSchema {
        return operationManager.getOperation(operationHandle).getResultSetSchema()
    }

    override fun close() {
        TODO("Not yet implemented")
    }
}

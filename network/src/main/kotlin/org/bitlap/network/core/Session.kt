package org.bitlap.network.core

import org.bitlap.common.BitlapConf
import org.bitlap.network.core.operation.OperationHandle
import org.bitlap.network.core.operation.OperationManager
import java.util.concurrent.atomic.AtomicBoolean

/**
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
interface Session {

    val sessionState: AtomicBoolean
    val sessionHandle: SessionHandle
    val password: String
    val username: String
    val creationTime: Long
    val sessionConf: BitlapConf
    val sessionManager: SessionManager

    var lastAccessTime: Long
    var operationManager: OperationManager

    /**
     * open Session
     * @param sessionConfMap
     * @return SessionHandle The Session handle
     */
    fun open(sessionConfMap: Map<String, String>?): SessionHandle

    /**
     * execute statement
     * @param statement
     * @param confOverlay
     * @return OperationHandle The Operate handle
     */
    fun executeStatement(
        sessionHandle: SessionHandle,
        statement: String,
        confOverlay: Map<String, String>?
    ): OperationHandle

    /**
     * execute statement
     * @param statement
     * @param confOverlay
     * @param queryTimeout
     * @return OperationHandle The Operate handle
     */
    fun executeStatement(
        sessionHandle: SessionHandle,
        statement: String,
        confOverlay: Map<String, String>?,
        queryTimeout: Long
    ): OperationHandle

    fun fetchResults(operationHandle: OperationHandle): RowSet
    fun getResultSetMetadata(operationHandle: OperationHandle): TableSchema
    // TODO: close OperationHandle

    /**
     * close Session
     */
    fun close()
}

package org.bitlap.network.core

import org.bitlap.network.core.operation.OperationHandle

/**
 * Interface definition of driver RPC.
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
interface NetworkService {

    fun openSession(username: String, password: String, configuration: Map<String, String>?): SessionHandle

    fun closeSession(sessionHandle: SessionHandle)

    fun executeStatement(
        sessionHandle: SessionHandle,
        statement: String,
        confOverlay: Map<String, String>?
    ): OperationHandle

    fun executeStatement(
        sessionHandle: SessionHandle,
        statement: String,
        confOverlay: Map<String, String>?,
        queryTimeout: Long
    ): OperationHandle

    fun fetchResults(opHandle: OperationHandle): RowSet

    fun getResultSetMetadata(
        opHandle: OperationHandle,
    ): TableSchema

    fun getColumns(
        sessionHandle: SessionHandle,
        tableName: String?,
        schemaName: String?,
        columnName: String?
    ): OperationHandle

    fun getTables(
        sessionHandle: SessionHandle,
        tableName: String?,
        schemaName: String?
    ): OperationHandle

    fun getSchemas(
        sessionHandle: SessionHandle,
    ): OperationHandle
}

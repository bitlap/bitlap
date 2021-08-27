package org.bitlap.network.core

import com.google.protobuf.ByteString

/**
 * Implementation of driver RPC core.
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
open class CLIServiceImpl(private val sessionManager: SessionManager) : CLIService {

    override fun openSession(
        username: String,
        password: String,
        configuration: Map<String, String>?
    ): SessionHandle {
        val session = sessionManager.openSession( username, password, configuration ?: mapOf())
        return session.sessionHandle
    }

    override fun closeSession(sessionHandle: SessionHandle) {
        sessionManager.closeSession(sessionHandle)
    }

    override fun executeStatement(
        sessionHandle: SessionHandle,
        statement: String,
        confOverlay: Map<String, String>?
    ): OperationHandle {
        val session = sessionManager.getSession(sessionHandle)
        sessionManager.refreshSession(sessionHandle, session)
        return session.executeStatement(statement, confOverlay)
    }

    override fun executeStatement(
        sessionHandle: SessionHandle,
        statement: String,
        confOverlay: Map<String, String>?,
        queryTimeout: Long
    ): OperationHandle {
        val session = sessionManager.getSession(sessionHandle)
        sessionManager.refreshSession(sessionHandle, session)
        return session.executeStatement(statement, confOverlay, queryTimeout)
    }

    override fun fetchResults(opHandle: OperationHandle): RowSet {
        return RowSet(
            listOf(
                Row(
                    listOf(
                        ByteString.copyFromUtf8(1112.toString()),
                        ByteString.copyFromUtf8("张三"),
                        ByteString.copyFromUtf8(12222.3232.toString())
                    )
                )
            )
        )
    }

    override fun getResultSetMetadata(opHandle: OperationHandle): TableSchema {
        return TableSchema(
            listOf(
                ColumnDesc("ID", TypeId.B_TYPE_ID_INT_TYPE),
                ColumnDesc("NAME", TypeId.B_TYPE_ID_STRING_TYPE),
                ColumnDesc("SALARY", TypeId.B_TYPE_ID_DOUBLE_TYPE)
            )
        )
    }

    override fun getColumns(
        sessionHandle: SessionHandle,
        tableName: String?,
        schemaName: String?,
        columnName: String?
    ): OperationHandle {
        return OperationHandle(OperationType.GET_COLUMNS)
    }

    override fun getTables(sessionHandle: SessionHandle, tableName: String?, schemaName: String?): OperationHandle {
        return OperationHandle(OperationType.GET_TABLES)
    }

    override fun getSchemas(sessionHandle: SessionHandle): OperationHandle {
        return OperationHandle(OperationType.GET_SCHEMAS)
    }
}

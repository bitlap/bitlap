package org.bitlap.network.core

/**
 * Implementation of driver RPC core.
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
open class NetworkServiceImpl(private val sessionManager: SessionManager) : NetworkService {

    override fun openSession(
        username: String,
        password: String,
        configuration: Map<String, String>?
    ): SessionHandle {
        val session = sessionManager.openSession(username, password, configuration ?: mapOf())
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
        return session.executeStatement(sessionHandle, statement, confOverlay)
    }

    override fun executeStatement(
        sessionHandle: SessionHandle,
        statement: String,
        confOverlay: Map<String, String>?,
        queryTimeout: Long
    ): OperationHandle {
        val session = sessionManager.getSession(sessionHandle)
        sessionManager.refreshSession(sessionHandle, session)
        return session.executeStatement(sessionHandle, statement, confOverlay, queryTimeout)
    }

    override fun fetchResults(opHandle: OperationHandle): RowSet {
        val sessionHandle = opHandle.sessionHandle
        val session = sessionManager.getSession(sessionHandle)
        sessionManager.refreshSession(sessionHandle, session)

        return session.fetchResults(opHandle)
//        return RowSet(
//            listOf(
//                Row(
//                    listOf(
//                        ByteString.copyFromUtf8(1112.toString()),
//                        ByteString.copyFromUtf8("张三"),
//                        ByteString.copyFromUtf8(12222.3232.toString()),
//                        ByteString.copyFromUtf8(1.toString()),
//                        ByteString.copyFromUtf8(99912211111113232L.toString()),
//                        ByteString.copyFromUtf8(false.toString()),
//                        ByteString.copyFromUtf8(1630332338000L.toString()),
//
//                        )
//                ),
//                Row(
//                    listOf(
//                        ByteString.copyFromUtf8(12222.toString()),
//                        ByteString.copyFromUtf8("李四"),
//                        ByteString.copyFromUtf8(10089.3232.toString()),
//                        ByteString.copyFromUtf8(11.toString()),
//                        ByteString.copyFromUtf8(99912211111113232L.toString()),
//                        ByteString.copyFromUtf8(false.toString()),
//                        ByteString.copyFromUtf8(1630332338000L.toString()),
//                    )
//                )
//            )
//        )
    }

    override fun getResultSetMetadata(opHandle: OperationHandle): TableSchema {
        val sessionHandle = opHandle.sessionHandle
        val session = sessionManager.getSession(sessionHandle)
        sessionManager.refreshSession(sessionHandle, session)
        return session.getResultSetMetadata(opHandle)
//        return TableSchema(
//            listOf(
//                ColumnDesc("ID", TypeId.B_TYPE_ID_INT_TYPE),
//                ColumnDesc("NAME", TypeId.B_TYPE_ID_STRING_TYPE),
//                ColumnDesc("SALARY", TypeId.B_TYPE_ID_DOUBLE_TYPE),
//                ColumnDesc("SHORT_COL", TypeId.B_TYPE_ID_SHORT_TYPE),
//                ColumnDesc("LONG_COL", TypeId.B_TYPE_ID_LONG_TYPE),
//                ColumnDesc("BOOLEAN_COL", TypeId.B_TYPE_ID_BOOLEAN_TYPE),
//                ColumnDesc("TIMESTAMP_COL", TypeId.B_TYPE_ID_TIMESTAMP_TYPE)
//
//            )
//        )
    }

    override fun getColumns(
        sessionHandle: SessionHandle,
        tableName: String?,
        schemaName: String?,
        columnName: String?
    ): OperationHandle {
        return OperationHandle(sessionHandle, HandleIdentifier(), OperationType.GET_COLUMNS)
    }

    override fun getTables(sessionHandle: SessionHandle, tableName: String?, schemaName: String?): OperationHandle {
        return OperationHandle(sessionHandle, HandleIdentifier(), OperationType.GET_TABLES)
    }

    override fun getSchemas(sessionHandle: SessionHandle): OperationHandle {
        return OperationHandle(sessionHandle, HandleIdentifier(), OperationType.GET_SCHEMAS)
    }
}

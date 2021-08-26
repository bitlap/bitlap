package org.bitlap.network.core

import org.bitlap.network.proto.driver.BColumn
import org.bitlap.network.proto.driver.BColumnDesc
import org.bitlap.network.proto.driver.BHandleIdentifier
import org.bitlap.network.proto.driver.BOperationHandle
import org.bitlap.network.proto.driver.BOperationType
import org.bitlap.network.proto.driver.BRow
import org.bitlap.network.proto.driver.BRowSet
import org.bitlap.network.proto.driver.BTableSchema
import org.bitlap.network.proto.driver.BTypeId
import java.util.UUID

/**
 * Implementation of driver RPC core.
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
open class BCLIService(private val sessionManager: SessionManager) : CLIService {

    // get session manager by conf
    // create session
    // execute statement by session
    // return OperationHandle
    // fetch results by OperationHandle

    override fun openSession(
        username: String,
        password: String,
        configuration: Map<String, String>?
    ): SessionHandle {
        return sessionManager.openSession(
            null, username, password, configuration ?: mapOf()
        ).sessionHandle
    }

    override fun closeSession(sessionHandle: SessionHandle) {
        sessionManager.closeSession(sessionHandle)
    }

    override fun executeStatement(
        sessionHandle: SessionHandle,
        statement: String,
        confOverlay: Map<String, String>?
    ): OperationHandle {
        val opHandle = BOperationHandle.newBuilder().setOperationType(BOperationType.B_OPERATION_TYPE_EXECUTE_STATEMENT)
            .setOperationId(
                BHandleIdentifier.newBuilder().setGuid(UUID.randomUUID().toString())
                    .setSecret(UUID.randomUUID().toString()).build()
            )
            .setHasResultSet(true).build()
        return OperationHandle(opHandle)
    }

    override fun executeStatement(
        sessionHandle: SessionHandle,
        statement: String,
        confOverlay: Map<String, String>?,
        queryTimeout: Long
    ): OperationHandle {
        val opHandle = BOperationHandle.newBuilder().setOperationType(BOperationType.B_OPERATION_TYPE_EXECUTE_STATEMENT)
            .setOperationId(
                BHandleIdentifier.newBuilder().setGuid(UUID.randomUUID().toString())
                    .setSecret(UUID.randomUUID().toString()).build()
            )
            .setHasResultSet(true).build()
        return OperationHandle(opHandle)
    }

    override fun fetchResults(opHandle: OperationHandle): BRowSet {
        // ID NAME
        // 1 zhangsan
        return BRowSet.newBuilder().addRows(BRow.newBuilder().addColVals("1").addColVals("zhangsan").build())
            .addColumns(BColumn.newBuilder().addStringColumn("ID").addStringColumn("NAME").build()).build()
    }

    override fun getResultSetMetadata(opHandle: OperationHandle): BTableSchema {
        val columnId = BColumnDesc.newBuilder().setColumnName("ID").setTypeDesc(BTypeId.B_TYPE_ID_STRING_TYPE).build()
        val columnName = BColumnDesc.newBuilder().setColumnName("NAME").setTypeDesc(BTypeId.B_TYPE_ID_STRING_TYPE).build()
        return BTableSchema.newBuilder().addColumns(columnId).addColumns(columnName).build()
    }
}

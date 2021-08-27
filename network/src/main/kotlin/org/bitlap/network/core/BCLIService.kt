package org.bitlap.network.core

import com.google.protobuf.ByteString
import org.bitlap.network.proto.driver.BColumn
import org.bitlap.network.proto.driver.BColumnDesc
import org.bitlap.network.proto.driver.BRow
import org.bitlap.network.proto.driver.BRowSet
import org.bitlap.network.proto.driver.BTableSchema
import org.bitlap.network.proto.driver.BTypeId

/**
 * Implementation of driver RPC core.
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
open class BCLIService(private val sessionManager: SessionManager) : CLIService {

    override fun openSession(
        username: String,
        password: String,
        configuration: Map<String, String>?
    ): SessionHandle {
        return sessionManager.openSession(null, username, password, configuration ?: mapOf()).sessionHandle
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

    override fun fetchResults(opHandle: OperationHandle): BRowSet {
        // ID NAME
        // 1 zhangsan
        // TODO get by session
        return BRowSet.newBuilder().addRows(
            BRow.newBuilder()
                .addColVals(ByteString.copyFromUtf8(1112.toString()))
                .addColVals(ByteString.copyFromUtf8("张三"))
                .addColVals(ByteString.copyFromUtf8(12222.3232.toString()))
                .build()
        ).addColumns(
            BColumn.newBuilder()
                .addStringColumn("ID")
                .addStringColumn("NAME")
                .addStringColumn("SALARY").build()
        ).build()
    }

    override fun getResultSetMetadata(opHandle: OperationHandle): BTableSchema {
        // TODO get by session
        val columnId = BColumnDesc.newBuilder().setColumnName("ID").setTypeDesc(BTypeId.B_TYPE_ID_INT_TYPE).build()
        val columnName =
            BColumnDesc.newBuilder().setColumnName("NAME").setTypeDesc(BTypeId.B_TYPE_ID_STRING_TYPE).build()
        val columnSalary =
            BColumnDesc.newBuilder().setColumnName("SALARY").setTypeDesc(BTypeId.B_TYPE_ID_DOUBLE_TYPE).build()
        return BTableSchema.newBuilder().addColumns(columnId).addColumns(columnName).addColumns(columnSalary).build()
    }
}

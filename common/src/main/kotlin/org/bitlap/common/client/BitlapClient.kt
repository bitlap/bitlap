package org.bitlap.common.client

import com.alipay.sofa.jraft.RouteTable
import com.alipay.sofa.jraft.conf.Configuration
import com.alipay.sofa.jraft.option.CliOptions
import com.alipay.sofa.jraft.rpc.impl.MarshallerHelper
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl
import com.alipay.sofa.jraft.util.RpcFactoryHelper
import com.google.protobuf.ProtocolStringList
import org.bitlap.common.proto.driver.BCloseSession
import org.bitlap.common.proto.driver.BExecuteStatement
import org.bitlap.common.proto.driver.BFetchResults
import org.bitlap.common.proto.driver.BGetColumns
import org.bitlap.common.proto.driver.BGetResultSetMetadata
import org.bitlap.common.proto.driver.BGetSchemas
import org.bitlap.common.proto.driver.BGetTables
import org.bitlap.common.proto.driver.BOpenSession
import org.bitlap.common.proto.driver.BOperationHandle
import org.bitlap.common.proto.driver.BSessionHandle
import org.bitlap.common.proto.driver.BStatus
import org.bitlap.common.proto.driver.BStatusCode
import org.bitlap.common.proto.driver.BTableSchema
import java.sql.SQLException
import java.util.Properties

/**
 * Wrap the rpc invoked
 *
 * @author 梦境迷离
 * @since 2021/8/22
 * @version 1.0
 */
object BitlapClient : RpcServiceSupport {

    private const val groupId: String = "bitlap-cluster"
    private const val timeout = 50000L

    // TODO we should verify status

    fun CliClientServiceImpl.openSession(
        conf: Configuration,
        info: Properties
    ): BSessionHandle {
        this.init(conf)
        val leader = RouteTable.getInstance().selectLeader(groupId)
        val result = this.rpcClient.invokeSync(
            leader.endpoint,
            BOpenSession.BOpenSessionReq.newBuilder().setUsername(info["user"].toString())
                .setPassword(info["password"].toString()).build(),
            timeout
        )
        result as BOpenSession.BOpenSessionResp
        verifySuccess(result.status)
        return result.sessionHandle
    }

    fun CliClientServiceImpl.closeSession(
        sessionHandle: BSessionHandle
    ) {
        val leader = RouteTable.getInstance().selectLeader(groupId)
        this.rpcClient.invokeAsync(
            leader.endpoint,
            BCloseSession.BCloseSessionReq.newBuilder().setSessionHandle(sessionHandle).build(),
            { result, _ ->
                result as BCloseSession.BCloseSessionResp
            },
            timeout
        )
    }

    fun CliClientServiceImpl.executeStatement(
        sessionHandle: BSessionHandle,
        statement: String,
    ): BOperationHandle {
        val leader = RouteTable.getInstance().selectLeader(groupId)
        val result = this.rpcClient.invokeSync(
            leader.endpoint,
            BExecuteStatement.BExecuteStatementReq.newBuilder().setSessionHandle(sessionHandle).setStatement(statement)
                .build(),
            timeout
        )
        result as BExecuteStatement.BExecuteStatementResp
        verifySuccess(result.status)
        return result.operationHandle
    }

    fun CliClientServiceImpl.fetchResults(
        operationHandle: BOperationHandle
    ): ProtocolStringList? {
        val leader = RouteTable.getInstance().selectLeader(groupId)
        val result = this.rpcClient.invokeSync(
            leader.endpoint,
            BFetchResults.BFetchResultsReq.newBuilder().setOperationHandle(operationHandle).build(),
            timeout
        )
        result as BFetchResults.BFetchResultsResp
        verifySuccess(result.status)
        return result.resultsList
    }

    fun CliClientServiceImpl.getSchemas(
        sessionHandle: BSessionHandle,
        catalogName: String?,
        schemaName: String?
    ): BOperationHandle {
        val leader = RouteTable.getInstance().selectLeader(groupId)
        val result = this.rpcClient.invokeSync(
            leader.endpoint,
            BGetSchemas.BGetSchemasReq.newBuilder().setSessionHandle(sessionHandle).setCatalogName(catalogName)
                .setSchemaName(schemaName).build(),
            timeout
        )
        result as BGetSchemas.BGetSchemasResp
        verifySuccess(result.status)
        return result.operationHandle
    }

    fun CliClientServiceImpl.getTables(
        sessionHandle: BSessionHandle,
        tableName: String?,
        schemaName: String?
    ): BOperationHandle {
        val leader = RouteTable.getInstance().selectLeader(groupId)
        val result = this.rpcClient.invokeSync(
            leader.endpoint,
            BGetTables.BGetTablesReq.newBuilder().setSessionHandle(sessionHandle).setTableName(tableName)
                .setSchemaName(schemaName).build(),
            timeout
        )
        result as BGetTables.BGetTablesResp
        verifySuccess(result.status)
        return result.operationHandle
    }

    fun CliClientServiceImpl.getColumns(
        sessionHandle: BSessionHandle,
        tableName: String?,
        schemaName: String?,
        columnName: String?
    ): BOperationHandle {
        val leader = RouteTable.getInstance().selectLeader(groupId)
        val result = this.rpcClient.invokeSync(
            leader.endpoint,
            BGetColumns.BGetColumnsReq.newBuilder().setSessionHandle(sessionHandle).setTableName(tableName)
                .setColumnName(columnName)
                .setSchemaName(schemaName).build(),
            timeout
        )
        result as BGetColumns.BGetColumnsResp
        verifySuccess(result.status)
        return result.operationHandle
    }

    fun CliClientServiceImpl.getResultSetMetadata(
        operationHandle: BOperationHandle,
    ): BTableSchema {
        val leader = RouteTable.getInstance().selectLeader(groupId)
        val result = this.rpcClient.invokeSync(
            leader.endpoint,
            BGetResultSetMetadata.BGetResultSetMetadataReq.newBuilder().setOperationHandle(operationHandle).build(),
            timeout
        )
        result as BGetResultSetMetadata.BGetResultSetMetadataResp
        verifySuccess(result.status)
        return result.schema
    }

    private fun CliClientServiceImpl.init(conf: Configuration) {
        RouteTable.getInstance().updateConfiguration(groupId, conf)
        this.init(CliOptions())
        check(RouteTable.getInstance().refreshLeader(this, groupId, 1000).isOk) { "Refresh leader failed" }
    }

    fun beforeInit() {
        registerMessageInstances(RpcServiceSupport.requestInstances()) {
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(it.first, it.second)
        }
        registerMessageInstances(RpcServiceSupport.responseInstances()) {
            MarshallerHelper.registerRespInstance(it.first, it.second)
        }
    }

    private fun verifySuccess(status: BStatus) {
        if (status.statusCode !== BStatusCode.B_STATUS_CODE_SUCCESS_STATUS) {
            throw SQLException(
                status.errorMessage,
                status.sqlState, status.errorCode
            )
        }
    }
}

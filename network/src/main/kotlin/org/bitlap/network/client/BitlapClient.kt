package org.bitlap.network.client

import com.alipay.sofa.jraft.RouteTable
import com.alipay.sofa.jraft.conf.Configuration
import com.alipay.sofa.jraft.option.CliOptions
import com.alipay.sofa.jraft.rpc.impl.MarshallerHelper
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl
import com.alipay.sofa.jraft.util.RpcFactoryHelper
import java.sql.SQLException
import java.util.Properties
import org.bitlap.network.NetworkHelper
import org.bitlap.network.proto.driver.BCloseSession
import org.bitlap.network.proto.driver.BExecuteStatement
import org.bitlap.network.proto.driver.BFetchResults
import org.bitlap.network.proto.driver.BGetColumns
import org.bitlap.network.proto.driver.BGetResultSetMetadata
import org.bitlap.network.proto.driver.BGetSchemas
import org.bitlap.network.proto.driver.BGetTables
import org.bitlap.network.proto.driver.BOpenSession
import org.bitlap.network.proto.driver.BOperationHandle
import org.bitlap.network.proto.driver.BSessionHandle
import org.bitlap.network.proto.driver.BStatus
import org.bitlap.network.proto.driver.BStatusCode
import org.bitlap.network.proto.driver.BTableSchema

/**
 * Wrap the rpc invoked
 *
 * @author 梦境迷离
 * @since 2021/8/22
 * @version 1.0
 */
object BitlapClient : NetworkHelper {

    private const val groupId: String = "bitlap-cluster"
    private const val timeout = 5000L

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
    ): BFetchResults.BFetchResultsResp {
        val leader = RouteTable.getInstance().selectLeader(groupId)
        val result = this.rpcClient.invokeSync(
            leader.endpoint,
            BFetchResults.BFetchResultsReq.newBuilder().setOperationHandle(operationHandle).build(),
            timeout
        )
        result as BFetchResults.BFetchResultsResp
        verifySuccess(result.status)
        return result
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
        registerMessageInstances(NetworkHelper.requestInstances()) {
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(it.first, it.second)
        }
        registerMessageInstances(NetworkHelper.responseInstances()) {
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

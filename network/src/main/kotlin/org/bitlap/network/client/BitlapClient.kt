package org.bitlap.network.client

import com.alipay.sofa.jraft.RouteTable
import com.alipay.sofa.jraft.conf.Configuration
import com.alipay.sofa.jraft.option.CliOptions
import com.alipay.sofa.jraft.rpc.impl.MarshallerHelper
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl
import com.alipay.sofa.jraft.util.RpcFactoryHelper
import org.bitlap.common.BitlapConf
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
import java.sql.SQLException
import java.util.Properties

/**
 * This class mainly wraps the RPC call procedure used inside JDBC.
 *
 * @author 梦境迷离
 * @since 2021/8/22
 * @version 1.0
 */
object BitlapClient : NetworkHelper {

    private val defaultConf by lazy { BitlapConf() }
    private val groupId: String = defaultConf.get(BitlapConf.NODE_GROUP_ID).let { if (it.isNullOrEmpty()) "bitlap-cluster" else it }
    private val timeout: Long = defaultConf.get(BitlapConf.NODE_RPC_TIMEOUT).let { if (it.isNullOrEmpty()) 5L else it.toLong() } * 1000
    private val raftTimeout: Int = defaultConf.get(BitlapConf.NODE_RAFT_TIMEOUT).let { if (it.isNullOrEmpty()) 1 else it.toInt() } * 1000

    /**
     * Used to open a session during JDBC connection initialization.
     */
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

    /**
     * Used to close the session when the JDBC connection is closed.
     */
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

    /**
     * Used to execute normal SQL by JDBC. Does not contain `?` placeholders.
     */
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

    /**
     *  Used for JDBC to get result set of the specified operation.
     */
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

    /**
     * Used for JDBC to get Schema of the specified operation.
     */
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

    /**
     * Used to initialize available nodes.
     */
    private fun CliClientServiceImpl.init(conf: Configuration) {
        RouteTable.getInstance().updateConfiguration(groupId, conf)
        this.init(CliOptions())
        check(RouteTable.getInstance().refreshLeader(this, groupId, raftTimeout).isOk) { "Refresh leader failed" }
    }

    fun beforeInit() {
        registerMessageInstances(NetworkHelper.requestInstances()) {
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(it.first, it.second)
        }
        registerMessageInstances(NetworkHelper.responseInstances()) {
            MarshallerHelper.registerRespInstance(it.first, it.second)
        }
    }

    /**
     * Used to verify whether the RPC result is correct.
     */
    private fun verifySuccess(status: BStatus) {
        if (status.statusCode !== BStatusCode.B_STATUS_CODE_SUCCESS_STATUS) {
            throw SQLException(
                status.errorMessage,
                status.sqlState, status.errorCode
            )
        }
    }
}

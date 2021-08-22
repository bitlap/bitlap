package org.bitlap.common.client

import com.alipay.sofa.jraft.RouteTable
import com.alipay.sofa.jraft.conf.Configuration
import com.alipay.sofa.jraft.option.CliOptions
import com.alipay.sofa.jraft.rpc.impl.MarshallerHelper
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl
import com.alipay.sofa.jraft.util.RpcFactoryHelper
import com.google.protobuf.ProtocolStringList
import java.util.Properties
import org.bitlap.common.proto.driver.BCloseSession
import org.bitlap.common.proto.driver.BExecuteStatement
import org.bitlap.common.proto.driver.BFetchResults
import org.bitlap.common.proto.driver.BOpenSession
import org.bitlap.common.proto.driver.BOperationHandle
import org.bitlap.common.proto.driver.BSessionHandle

/**
 *
 * @author 梦境迷离
 * @since 2021/8/22
 * @version 1.0
 */
object BitlapClient : RpcServiceSupport {

    private val groupId: String = "bitlap-cluster"
    private val timeout = 50000L

    fun CliClientServiceImpl.openSession(
        conf: Configuration,
        info: Properties?
    ): BSessionHandle? {
        this.init(conf)
        val leader = RouteTable.getInstance().selectLeader(groupId)
        val result = this.rpcClient.invokeSync(
            leader.endpoint,
            BOpenSession.BOpenSessionReq.newBuilder().setUsername(info?.get("user").toString())
                .setPassword(info?.get("password").toString()).build(),
            timeout
        )
        result as BOpenSession.BOpenSessionResp
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
                println("Close session: $result")
            },
            timeout
        )
    }

    fun CliClientServiceImpl.executeStatement(
        sessionHandle: BSessionHandle,
        statement: String,
    ): BOperationHandle? {
        val leader = RouteTable.getInstance().selectLeader(groupId)
        val result = this.rpcClient.invokeSync(
            leader.endpoint,
            BExecuteStatement.BExecuteStatementReq.newBuilder().setSessionHandle(sessionHandle).setStatement(statement)
                .build(),
            timeout
        )
        result as BExecuteStatement.BExecuteStatementResp
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
        return result.resultsList
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
}

package org.bitlap.network.processor

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcRequestClosure
import com.google.protobuf.Message
import org.bitlap.network.core.NetworkService
import org.bitlap.network.core.SessionHandle
import org.bitlap.network.proto.driver.BExecuteStatement.BExecuteStatementReq
import org.bitlap.network.proto.driver.BExecuteStatement.BExecuteStatementResp
import java.util.concurrent.Executor

/**
 * ExecuteStatement
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class ExecuteStatementProcessor(
    private val networkService: NetworkService,
    executor: Executor? = null
) : BitlapRpcProcessor<BExecuteStatementReq>(executor, BExecuteStatementResp.getDefaultInstance()) {

    override fun processRequest(request: BExecuteStatementReq, done: RpcRequestClosure): Message {
        val sessionHandle = request.sessionHandle
        val statement = request.statement
        val confOverlayMap = request.confOverlayMap
        val operationHandle = networkService.executeStatement(SessionHandle(sessionHandle), statement, confOverlayMap)
        return BExecuteStatementResp.newBuilder()
            .setOperationHandle(operationHandle.toBOperationHandle())
            .setStatus(success()).build()
    }

    override fun processError(rpcCtx: RpcContext, exception: Exception): Message {
        return BExecuteStatementResp.newBuilder().setStatus(error(exception)).build()
    }

    override fun interest(): String = BExecuteStatementReq::class.java.name
}

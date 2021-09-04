package org.bitlap.network.processor

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcRequestClosure
import com.google.protobuf.Message
import org.bitlap.network.core.NetworkService
import org.bitlap.network.core.SessionHandle
import org.bitlap.network.proto.driver.BCloseSession
import java.util.concurrent.Executor

/**
 * CloseSession
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class CloseSessionProcessor(
    private val networkService: NetworkService,
    executor: Executor? = null,
) : BitlapRpcProcessor<BCloseSession.BCloseSessionReq>(executor, BCloseSession.BCloseSessionResp.getDefaultInstance()) {

    override fun processRequest(request: BCloseSession.BCloseSessionReq, done: RpcRequestClosure): Message {
        val sessionHandle = request.sessionHandle
        networkService.closeSession(SessionHandle(sessionHandle))
        return BCloseSession.BCloseSessionResp.newBuilder().setStatus(success()).build()
    }

    override fun processError(rpcCtx: RpcContext, exception: Exception): Message {
        return BCloseSession.BCloseSessionResp.newBuilder().setStatus(error(exception)).build()
    }

    override fun interest(): String = BCloseSession.BCloseSessionReq::class.java.name
}

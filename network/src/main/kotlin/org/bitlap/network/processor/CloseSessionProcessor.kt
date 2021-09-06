package org.bitlap.network.processor

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcRequestClosure
import com.google.protobuf.Message
import org.bitlap.network.core.NetworkService
import org.bitlap.network.core.SessionHandle
import org.bitlap.network.proto.driver.BCloseSession.BCloseSessionReq
import org.bitlap.network.proto.driver.BCloseSession.BCloseSessionResp
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
) : BitlapRpcProcessor<BCloseSessionReq>(executor, BCloseSessionResp.getDefaultInstance()) {

    override fun processRequest(request: BCloseSessionReq, done: RpcRequestClosure): Message {
        val sessionHandle = request.sessionHandle
        networkService.closeSession(SessionHandle(sessionHandle))
        return BCloseSessionResp.newBuilder().setStatus(success()).build()
    }

    override fun processError(rpcCtx: RpcContext, exception: Exception): Message {
        return BCloseSessionResp.newBuilder().setStatus(error(exception)).build()
    }

    override fun interest(): String = BCloseSessionReq::class.java.name
}

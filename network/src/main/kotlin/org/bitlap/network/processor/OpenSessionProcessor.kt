package org.bitlap.network.processor

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcRequestClosure
import com.google.protobuf.Message
import org.bitlap.network.core.NetworkService
import org.bitlap.network.proto.driver.BOpenSession.BOpenSessionReq
import org.bitlap.network.proto.driver.BOpenSession.BOpenSessionResp
import java.util.concurrent.Executor

/**
 * OpenSession
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class OpenSessionProcessor(
    private val networkService: NetworkService,
    executor: Executor? = null,
) : BitlapRpcProcessor<BOpenSessionReq>(executor, BOpenSessionResp.getDefaultInstance()) {

    override fun processRequest(request: BOpenSessionReq, done: RpcRequestClosure): Message {
        val username = request.username
        val password = request.password
        val configurationMap = request.configurationMap
        val sessionHandle = networkService.openSession(username, password, configurationMap)
        return BOpenSessionResp.newBuilder()
            .setSessionHandle(sessionHandle.toBSessionHandle())
            .setStatus(success()).build()
    }

    override fun processError(rpcCtx: RpcContext, exception: Exception): Message {
        return BOpenSessionResp.newBuilder().setStatus(error(exception)).build()
    }

    override fun interest(): String = BOpenSessionReq::class.java.name
}

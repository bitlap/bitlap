package org.bitlap.network.processor

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcRequestClosure
import com.google.protobuf.Message
import org.bitlap.network.core.NetworkService
import org.bitlap.network.core.SessionHandle
import org.bitlap.network.proto.driver.BGetSchemas.BGetSchemasReq
import org.bitlap.network.proto.driver.BGetSchemas.BGetSchemasResp
import java.util.concurrent.Executor

/**
 * GetSchemas
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class GetSchemasProcessor(
    private val networkService: NetworkService,
    executor: Executor? = null
) : BitlapRpcProcessor<BGetSchemasReq>(executor, BGetSchemasResp.getDefaultInstance()) {

    override fun processRequest(request: BGetSchemasReq, done: RpcRequestClosure): Message {
        val sessionHandle = request.sessionHandle
        val result = networkService.getSchemas(SessionHandle(sessionHandle))
        return BGetSchemasResp.newBuilder()
            .setStatus(success()).setOperationHandle(result.toBOperationHandle()).build()
    }

    override fun processError(rpcCtx: RpcContext, exception: Exception): Message {
        return BGetSchemasResp.newBuilder().setStatus(error(exception)).build()
    }

    override fun interest(): String = BGetSchemasReq::class.java.name
}

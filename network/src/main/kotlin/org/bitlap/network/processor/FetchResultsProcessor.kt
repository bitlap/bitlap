package org.bitlap.network.processor

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcRequestClosure
import com.google.protobuf.Message
import org.bitlap.network.core.NetworkService
import org.bitlap.network.core.OperationHandle
import org.bitlap.network.proto.driver.BFetchResults.BFetchResultsReq
import org.bitlap.network.proto.driver.BFetchResults.BFetchResultsResp
import java.util.concurrent.Executor

/**
 * FetchResults
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class FetchResultsProcessor(
    private val networkService: NetworkService,
    executor: Executor? = null,
) : BitlapRpcProcessor<BFetchResultsReq>(executor, BFetchResultsResp.getDefaultInstance()) {

    override fun processRequest(request: BFetchResultsReq, done: RpcRequestClosure): Message {
        val sessionHandle = request.operationHandle.sessionHandle
        val operationHandle = request.operationHandle
        val result = networkService.fetchResults(OperationHandle(sessionHandle, operationHandle))
        return BFetchResultsResp.newBuilder()
            .setHasMoreRows(false)
            .setStatus(success()).setResults(result.toBRowSet()).build()
    }

    override fun processError(rpcCtx: RpcContext, exception: Exception): Message {
        return BFetchResultsResp.newBuilder().setStatus(error(exception)).build()
    }

    override fun interest(): String = BFetchResultsReq::class.java.name
}

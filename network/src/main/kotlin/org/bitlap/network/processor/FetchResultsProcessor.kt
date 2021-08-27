package org.bitlap.network.processor

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcProcessor
import org.bitlap.common.exception.BitlapException
import org.bitlap.network.core.CLIService
import org.bitlap.network.core.OperationHandle
import org.bitlap.network.proto.driver.BFetchResults

/**
 * FetchResults
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class FetchResultsProcessor(private val cliService: CLIService) :
    RpcProcessor<BFetchResults.BFetchResultsReq>,
    ProcessorHelper {
    override fun handleRequest(rpcCtx: RpcContext, request: BFetchResults.BFetchResultsReq) {
        val operationHandle = request.operationHandle
        val resp: BFetchResults.BFetchResultsResp = try {
            val result = cliService.fetchResults(OperationHandle((operationHandle)))
            BFetchResults.BFetchResultsResp.newBuilder()
                .setHasMoreRows(false)
                .setStatus(success()).setResults(result.toBRowSet()).build()
        } catch (e: BitlapException) {
            e.printStackTrace()
            BFetchResults.BFetchResultsResp.newBuilder().setStatus(error()).build()
        }
        rpcCtx.sendResponse(resp)
    }

    override fun interest(): String = BFetchResults.BFetchResultsReq::class.java.name
}

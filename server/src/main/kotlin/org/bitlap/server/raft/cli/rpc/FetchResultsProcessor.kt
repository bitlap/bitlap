package org.bitlap.server.raft.cli.rpc

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcProcessor
import org.bitlap.common.proto.driver.BFetchResults
import org.bitlap.server.raft.cli.BSQLException
import org.bitlap.server.raft.cli.CLIService
import org.bitlap.server.raft.cli.OperationHandle

/**
 * FetchResults
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class FetchResultsProcessor(private val cliService: CLIService) :
    RpcProcessor<BFetchResults.BFetchResultsReq>,
    BaseProcessor {
    override fun handleRequest(rpcCtx: RpcContext, request: BFetchResults.BFetchResultsReq) {
        val operationHandle = request.operationHandle
        val result = cliService.fetchResults(OperationHandle((operationHandle)))
        val resp: BFetchResults.BFetchResultsResp? = try {
            BFetchResults.BFetchResultsResp.newBuilder()
                .setHasMoreRows(false)
                .setStatus(success()).addAllResults(result).build()
        } catch (e: BSQLException) {
            BFetchResults.BFetchResultsResp.newBuilder().setStatus(error()).build()
        }
        rpcCtx.sendResponse(resp)
    }

    override fun interest(): String = BFetchResults.BFetchResultsReq::class.java.name
}

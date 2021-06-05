package org.bitlap.server.raft.rpc

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcProcessor
import org.bitlap.common.proto.driver.BFetchResults

/**
 * FetchResults
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class FetchResultsProcessor : RpcProcessor<BFetchResults.BFetchResultsReq> {
    override fun handleRequest(rpcCtx: RpcContext, request: BFetchResults.BFetchResultsReq) {
        TODO("Not yet implemented")
    }

    override fun interest(): String = BFetchResults.BFetchResultsReq::class.java.name
}

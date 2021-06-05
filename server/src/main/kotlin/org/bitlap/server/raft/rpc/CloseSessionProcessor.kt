package org.bitlap.server.raft.rpc

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcProcessor
import org.bitlap.common.proto.driver.BCloseSession

/**
 * CloseSession
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class CloseSessionProcessor : RpcProcessor<BCloseSession.BCloseSessionReq> {

    override fun handleRequest(rpcCtx: RpcContext, request: BCloseSession.BCloseSessionReq) {
        TODO("Not yet implemented")
    }

    override fun interest(): String = BCloseSession.BCloseSessionReq::class.java.name
}

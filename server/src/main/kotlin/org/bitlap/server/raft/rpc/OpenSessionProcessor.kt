package org.bitlap.server.raft.rpc

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcProcessor
import org.bitlap.common.proto.driver.BOpenSession

/**
 * OpenSession
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class OpenSessionProcessor : RpcProcessor<BOpenSession.BOpenSessionReq> {
    override fun handleRequest(rpcCtx: RpcContext, request: BOpenSession.BOpenSessionReq) {
        TODO("Not yet implemented")
    }

    override fun interest(): String = BOpenSession.BOpenSessionReq::class.java.name
}

package org.bitlap.server.raft.cli

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcProcessor
import org.bitlap.common.proto.rpc.HelloRpcPB

/**
 * Desc: Hello rpc processor
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/4/23
 */
class HelloRpcProcessor : RpcProcessor<HelloRpcPB.Req> {
    /**
     * Async to handle request with [RpcContext].
     *
     * @param rpcCtx  the rpc context
     * @param request the request
     */
    override fun handleRequest(rpcCtx: RpcContext, request: HelloRpcPB.Req) {
        val ping = request.ping
        rpcCtx.sendResponse(HelloRpcPB.Res.newBuilder().setPong("Hello $ping").build())
    }

    override fun interest(): String {
        return HelloRpcPB.Req::class.java.name
    }
}

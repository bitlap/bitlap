package org.bitlap.server.raft.cli.rpc

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcProcessor
import org.bitlap.common.proto.driver.BCloseSession
import org.bitlap.server.raft.cli.BSQLException
import org.bitlap.server.raft.cli.CLIService
import org.bitlap.server.raft.cli.SessionHandle

/**
 * CloseSession
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class CloseSessionProcessor(private val cliService: CLIService) :
    RpcProcessor<BCloseSession.BCloseSessionReq>,
    BaseProcessor {
    override fun handleRequest(rpcCtx: RpcContext, request: BCloseSession.BCloseSessionReq) {
        val sessionHandle = request.sessionHandle
        cliService.closeSession(SessionHandle(sessionHandle))
        val resp: BCloseSession.BCloseSessionResp? = try {
            BCloseSession.BCloseSessionResp.newBuilder()
                .setStatus(success()).build()
        } catch (e: BSQLException) {
            BCloseSession.BCloseSessionResp.newBuilder()
                .setStatus(error()).build()
        }
        rpcCtx.sendResponse(resp)
    }

    override fun interest(): String = BCloseSession.BCloseSessionReq::class.java.name
}

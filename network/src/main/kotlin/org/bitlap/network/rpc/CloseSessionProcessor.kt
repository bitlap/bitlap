package org.bitlap.network.rpc

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcProcessor
import org.bitlap.common.exception.BitlapException
import org.bitlap.network.core.CLIService
import org.bitlap.network.core.SessionHandle
import org.bitlap.network.proto.driver.BCloseSession

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
        val resp: BCloseSession.BCloseSessionResp = try {
            cliService.closeSession(SessionHandle(sessionHandle))
            BCloseSession.BCloseSessionResp.newBuilder()
                .setStatus(success()).build()
        } catch (e: BitlapException) {
            BCloseSession.BCloseSessionResp.newBuilder()
                .setStatus(error()).build()
        }
        rpcCtx.sendResponse(resp)
    }

    override fun interest(): String = BCloseSession.BCloseSessionReq::class.java.name
}

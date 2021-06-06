package org.bitlap.server.raft.cli.rpc

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcProcessor
import org.bitlap.common.proto.driver.BOpenSession
import org.bitlap.server.raft.cli.BSQLException
import org.bitlap.server.raft.cli.CLIService

/**
 * OpenSession
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class OpenSessionProcessor(private val cliService: CLIService) :
    RpcProcessor<BOpenSession.BOpenSessionReq>,
    BaseProcessor {
    override fun handleRequest(rpcCtx: RpcContext, request: BOpenSession.BOpenSessionReq) {
        val username = request.username
        val password = request.password
        val configurationMap = request.configurationMap
        val sessionHandle = cliService.openSession(username, password, configurationMap)
        val resp: BOpenSession.BOpenSessionResp? = try {
            BOpenSession.BOpenSessionResp.newBuilder()
                .setSessionHandle(sessionHandle?.toBSessionHandle())
                .setStatus(success()).build()
        } catch (e: BSQLException) {
            BOpenSession.BOpenSessionResp.newBuilder().setStatus(error()).build()
        }
        rpcCtx.sendResponse(resp)
    }

    override fun interest(): String = BOpenSession.BOpenSessionReq::class.java.name
}

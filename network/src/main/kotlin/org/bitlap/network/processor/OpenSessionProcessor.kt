package org.bitlap.network.processor

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcProcessor
import org.bitlap.common.exception.BitlapException
import org.bitlap.network.core.CLIService
import org.bitlap.network.proto.driver.BOpenSession

/**
 * OpenSession
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class OpenSessionProcessor(private val cliService: CLIService) :
    RpcProcessor<BOpenSession.BOpenSessionReq>,
    ProcessorHelper {
    override fun handleRequest(rpcCtx: RpcContext, request: BOpenSession.BOpenSessionReq) {
        val username = request.username
        val password = request.password
        val configurationMap = request.configurationMap
        val resp: BOpenSession.BOpenSessionResp = try {
            val sessionHandle = cliService.openSession(username, password, configurationMap)
            BOpenSession.BOpenSessionResp.newBuilder()
                .setSessionHandle(sessionHandle.toBSessionHandle())
                .setStatus(success()).build()
        } catch (e: BitlapException) {
            e.printStackTrace()
            BOpenSession.BOpenSessionResp.newBuilder().setStatus(error()).build()
        }
        rpcCtx.sendResponse(resp)
    }

    override fun interest(): String = BOpenSession.BOpenSessionReq::class.java.name
}

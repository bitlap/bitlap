package org.bitlap.network.processor

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcProcessor
import org.bitlap.common.exception.BitlapException
import org.bitlap.network.core.CLIService
import org.bitlap.network.core.SessionHandle
import org.bitlap.network.proto.driver.BExecuteStatement

/**
 * ExecuteStatement
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class ExecuteStatementProcessor(private val cliService: CLIService) :
    RpcProcessor<BExecuteStatement.BExecuteStatementReq>, ProcessorHelper {
    override fun handleRequest(rpcCtx: RpcContext, request: BExecuteStatement.BExecuteStatementReq) {
        val sessionHandle = request.sessionHandle
        val statement = request.statement
        val confOverlayMap = request.confOverlayMap
        val resp: BExecuteStatement.BExecuteStatementResp = try {
            val operationHandle = cliService.executeStatement(SessionHandle(sessionHandle), statement, confOverlayMap)
            BExecuteStatement.BExecuteStatementResp.newBuilder()
                .setOperationHandle(operationHandle.toBOperationHandle())
                .setStatus(success()).build()
        } catch (e: BitlapException) {
            e.printStackTrace()
            BExecuteStatement.BExecuteStatementResp.newBuilder().setStatus(error()).build()
        }
        rpcCtx.sendResponse(resp)
    }

    override fun interest(): String = BExecuteStatement.BExecuteStatementReq::class.java.name
}

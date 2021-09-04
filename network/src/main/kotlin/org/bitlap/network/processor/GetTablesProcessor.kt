package org.bitlap.network.processor

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcProcessor
import org.bitlap.common.exception.BitlapException
import org.bitlap.network.core.NetworkService
import org.bitlap.network.core.SessionHandle
import org.bitlap.network.proto.driver.BGetTables

/**
 * GetTables
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class GetTablesProcessor(private val networkService: NetworkService) :
    RpcProcessor<BGetTables.BGetTablesReq>,
    ProcessorHelper {
    override fun handleRequest(rpcCtx: RpcContext, request: BGetTables.BGetTablesReq) {
        val resp: BGetTables.BGetTablesResp = try {
            val result =
                networkService.getTables(SessionHandle((request.sessionHandle)), request.tableName, request.schemaName)
            BGetTables.BGetTablesResp.newBuilder()
                .setStatus(success()).setOperationHandle(result.toBOperationHandle(request.sessionHandle)).build()
        } catch (e: BitlapException) {
            e.printStackTrace()
            BGetTables.BGetTablesResp.newBuilder().setStatus(error()).build()
        }
        rpcCtx.sendResponse(resp)
    }

    override fun interest(): String = BGetTables.BGetTablesReq::class.java.name
}

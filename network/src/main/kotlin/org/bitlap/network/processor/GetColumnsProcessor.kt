package org.bitlap.network.processor

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcProcessor
import org.bitlap.common.exception.BitlapException
import org.bitlap.network.core.NetworkService
import org.bitlap.network.core.SessionHandle
import org.bitlap.network.proto.driver.BGetColumns

/**
 * GetColumns
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class GetColumnsProcessor(private val networkService: NetworkService) :
    RpcProcessor<BGetColumns.BGetColumnsReq>,
    ProcessorHelper {
    override fun handleRequest(rpcCtx: RpcContext, request: BGetColumns.BGetColumnsReq) {
        val resp: BGetColumns.BGetColumnsResp = try {
            val result =
                networkService.getColumns(
                    SessionHandle(request.sessionHandle),
                    request.tableName,
                    request.schemaName,
                    request.columnName
                )
            BGetColumns.BGetColumnsResp.newBuilder()
                .setStatus(success()).setOperationHandle(result.toBOperationHandle()).build()
        } catch (e: BitlapException) {
            e.printStackTrace()
            BGetColumns.BGetColumnsResp.newBuilder().setStatus(error()).build()
        }
        rpcCtx.sendResponse(resp)
    }

    override fun interest(): String = BGetColumns.BGetColumnsReq::class.java.name
}

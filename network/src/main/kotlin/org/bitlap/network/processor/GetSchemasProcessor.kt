package org.bitlap.network.processor

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcProcessor
import org.bitlap.common.exception.BitlapException
import org.bitlap.network.core.NetworkService
import org.bitlap.network.core.SessionHandle
import org.bitlap.network.proto.driver.BGetSchemas

/**
 * GetSchemas
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class GetSchemasProcessor(private val networkService: NetworkService) :
    RpcProcessor<BGetSchemas.BGetSchemasReq>,
    ProcessorHelper {
    override fun handleRequest(rpcCtx: RpcContext, request: BGetSchemas.BGetSchemasReq) {
        val sessionHandle = request.sessionHandle
        val resp: BGetSchemas.BGetSchemasResp = try {
            val result = networkService.getSchemas(SessionHandle(sessionHandle))
            BGetSchemas.BGetSchemasResp.newBuilder()
                .setStatus(success()).setOperationHandle(result.toBOperationHandle(sessionHandle)).build()
        } catch (e: BitlapException) {
            e.printStackTrace()
            BGetSchemas.BGetSchemasResp.newBuilder().setStatus(error()).build()
        }
        rpcCtx.sendResponse(resp)
    }

    override fun interest(): String = BGetSchemas.BGetSchemasReq::class.java.name
}

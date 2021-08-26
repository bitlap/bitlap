package org.bitlap.network.rpc

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcProcessor
import org.bitlap.common.exception.BitlapException
import org.bitlap.network.core.CLIService
import org.bitlap.network.core.OperationHandle
import org.bitlap.network.proto.driver.BGetResultSetMetadata

/**
 * GetResultSetMetadata
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class GetResultSetMetaDataProcessor(private val cliService: CLIService) :
    RpcProcessor<BGetResultSetMetadata.BGetResultSetMetadataReq>,
    BaseProcessor {
    override fun handleRequest(rpcCtx: RpcContext, request: BGetResultSetMetadata.BGetResultSetMetadataReq) {
        val operationHandle = request.operationHandle
        val resp: BGetResultSetMetadata.BGetResultSetMetadataResp = try {
            val result = cliService.getResultSetMetadata(OperationHandle((operationHandle)))
            BGetResultSetMetadata.BGetResultSetMetadataResp.newBuilder()
                .setStatus(success()).setSchema(result).build()
        } catch (e: BitlapException) {
            BGetResultSetMetadata.BGetResultSetMetadataResp.newBuilder().setStatus(error()).build()
        }
        rpcCtx.sendResponse(resp)
    }

    override fun interest(): String = BGetResultSetMetadata.BGetResultSetMetadataReq::class.java.name
}

package org.bitlap.network.processor

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcRequestClosure
import com.google.protobuf.Message
import org.bitlap.network.core.NetworkService
import org.bitlap.network.core.operation.OperationHandle
import org.bitlap.network.proto.driver.BGetResultSetMetadata.BGetResultSetMetadataReq
import org.bitlap.network.proto.driver.BGetResultSetMetadata.BGetResultSetMetadataResp
import java.util.concurrent.Executor

/**
 * GetResultSetMetadata
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class GetResultSetMetaDataProcessor(
    private val networkService: NetworkService,
    executor: Executor? = null,
) : BitlapRpcProcessor<BGetResultSetMetadataReq>(executor, BGetResultSetMetadataResp.getDefaultInstance()) {

    override fun processRequest(request: BGetResultSetMetadataReq, done: RpcRequestClosure): Message {
        val operationHandle = request.operationHandle
        val result = networkService.getResultSetMetadata(OperationHandle(operationHandle))
        return BGetResultSetMetadataResp.newBuilder().setStatus(success()).setSchema(result.toBTableSchema()).build()
    }

    override fun processError(rpcCtx: RpcContext, exception: Exception): Message {
        return BGetResultSetMetadataResp.newBuilder().setStatus(error(exception)).build()
    }

    override fun interest(): String = BGetResultSetMetadataReq::class.java.name
}

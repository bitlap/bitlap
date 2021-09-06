package org.bitlap.network.processor

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcRequestClosure
import com.google.protobuf.Message
import org.bitlap.network.core.NetworkService
import org.bitlap.network.core.SessionHandle
import org.bitlap.network.proto.driver.BGetColumns.BGetColumnsReq
import org.bitlap.network.proto.driver.BGetColumns.BGetColumnsResp
import java.util.concurrent.Executor

/**
 * GetColumns
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class GetColumnsProcessor(
    private val networkService: NetworkService,
    executor: Executor? = null,
) : BitlapRpcProcessor<BGetColumnsReq>(executor, BGetColumnsResp.getDefaultInstance()) {

    override fun processRequest(request: BGetColumnsReq, done: RpcRequestClosure): Message {
        val result =
            networkService.getColumns(
                SessionHandle(request.sessionHandle),
                request.tableName,
                request.schemaName,
                request.columnName
            )
        return BGetColumnsResp.newBuilder()
            .setStatus(success()).setOperationHandle(result.toBOperationHandle()).build()
    }

    override fun processError(rpcCtx: RpcContext, exception: Exception): Message {
        return BGetColumnsResp.newBuilder().setStatus(error(exception)).build()
    }

    override fun interest(): String = BGetColumnsReq::class.java.name
}

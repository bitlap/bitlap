package org.bitlap.network.processor

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcRequestClosure
import com.google.protobuf.Message
import org.bitlap.network.core.NetworkService
import org.bitlap.network.core.SessionHandle
import org.bitlap.network.proto.driver.BGetTables.BGetTablesReq
import org.bitlap.network.proto.driver.BGetTables.BGetTablesResp
import java.util.concurrent.Executor

/**
 * GetTables
 *
 * @author 梦境迷离
 * @since 2021/6/5
 * @version 1.0
 */
class GetTablesProcessor(
    private val networkService: NetworkService,
    executor: Executor? = null,
) : BitlapRpcProcessor<BGetTablesReq>(executor, BGetTablesResp.getDefaultInstance()) {

    override fun processRequest(request: BGetTablesReq, done: RpcRequestClosure): Message {
        val result =
            networkService.getTables(SessionHandle((request.sessionHandle)), request.tableName, request.schemaName)
        return BGetTablesResp.newBuilder()
            .setStatus(success()).setOperationHandle(result.toBOperationHandle()).build()
    }

    override fun processError(rpcCtx: RpcContext, exception: Exception): Message {
        return BGetTablesResp.newBuilder().setStatus(error()).build()
    }

    override fun interest(): String = BGetTablesReq::class.java.name
}

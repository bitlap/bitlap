package org.bitlap.net.processor

import com.alipay.sofa.jraft.rpc.{ RpcContext, RpcRequestClosure }
import com.google.protobuf.Message
import org.bitlap.net.{ NetworkService, handles }
import org.bitlap.network.proto.driver.BGetTables.{ BGetTablesReq, BGetTablesResp }

import java.util.concurrent.Executor

/**
 *
 * @author 梦境迷离
 * @since 2021/11/21
 * @version 1.0
 */
class GetTablesProcessor(private val networkService: NetworkService, executor: Executor = null)
  extends BitlapRpcProcessor[BGetTablesReq](executor, BGetTablesResp.getDefaultInstance) {

  override def processRequest(request: BGetTablesReq, done: RpcRequestClosure): Message = {
    val result =
      networkService.getTables(new handles.SessionHandle((request.getSessionHandle))
        , request.getTableName, request.getSchemaName)
    BGetTablesResp.newBuilder()
      .setStatus(success()).setOperationHandle(result.toBOperationHandle()).build()
  }

  override def processError(rpcCtx: RpcContext, exception: Exception): Message = {
    BGetTablesResp.newBuilder().setStatus(error()).build()
  }

  override def interest(): String = classOf[BGetTablesReq].getName
}


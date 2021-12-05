package org.bitlap.net.processor

import com.alipay.sofa.jraft.rpc.{ RpcContext, RpcRequestClosure }
import com.google.protobuf.Message
import org.bitlap.net.{ NetworkService, handles }
import org.bitlap.network.proto.driver.BGetSchemas.{ BGetSchemasReq, BGetSchemasResp }

import java.util.concurrent.Executor

/**
 *
 * @author 梦境迷离
 * @since 2021/11/21
 * @version 1.0
 */
class GetSchemasProcessor(private val networkService: NetworkService, executor: Executor = null)
  extends BitlapRpcProcessor[BGetSchemasReq](executor, BGetSchemasResp.getDefaultInstance) {

  override def processRequest(request: BGetSchemasReq, done: RpcRequestClosure): Message = {
    val sessionHandle = request.getSessionHandle
    val result = networkService.getSchemas(new handles.SessionHandle(sessionHandle))
    BGetSchemasResp.newBuilder()
      .setStatus(success()).setOperationHandle(result.toBOperationHandle()).build()
  }

  override def processError(rpcCtx: RpcContext, exception: Exception): Message = {
    BGetSchemasResp.newBuilder().setStatus(error(exception)).build()
  }

  override def interest(): String = classOf[BGetSchemasReq].getName
}


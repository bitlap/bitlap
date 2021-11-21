package org.bitlap.net.processor

import com.alipay.sofa.jraft.rpc.{ RpcContext, RpcRequestClosure }
import com.google.protobuf.Message
import org.bitlap.net.{ NetworkService, handles }
import org.bitlap.network.proto.driver.BGetColumns.{ BGetColumnsReq, BGetColumnsResp }

import java.util.concurrent.Executor

/**
 *
 * @author 梦境迷离
 * @since 2021/11/21
 * @version 1.0
 */
class GetColumnsProcessor(private val networkService: NetworkService, executor: Executor = null)
  extends BitlapRpcProcessor[BGetColumnsReq](executor, BGetColumnsResp.getDefaultInstance)
    with ProcessorHelper {

  override def processRequest(request: BGetColumnsReq, done: RpcRequestClosure): Message = {
    val result =
      networkService.getColumns(
        new handles.SessionHandle(request.getSessionHandle),
        request.getTableName,
        request.getSchemaName,
        request.getColumnName
      )
    BGetColumnsResp.newBuilder()
      .setStatus(success()).setOperationHandle(result.toBOperationHandle()).build()
  }

  override def processError(rpcCtx: RpcContext, exception: Exception): Message = BGetColumnsResp.newBuilder().setStatus(error(exception)).build()

  override def interest(): String = classOf[BGetColumnsReq].getName

}

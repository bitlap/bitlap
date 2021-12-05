package org.bitlap.net.processor

import com.alipay.sofa.jraft.rpc.{ RpcContext, RpcRequestClosure }
import com.google.protobuf.Message
import org.bitlap.net.{ NetworkService, handles }
import org.bitlap.network.proto.driver.BGetResultSetMetadata.{ BGetResultSetMetadataReq, BGetResultSetMetadataResp }

import java.util.concurrent.Executor

/**
 *
 * @author 梦境迷离
 * @since 2021/11/21
 * @version 1.0
 */
class GetResultSetMetaDataProcessor(private val networkService: NetworkService, executor: Executor = null)
  extends BitlapRpcProcessor[BGetResultSetMetadataReq](executor, BGetResultSetMetadataResp.getDefaultInstance) {

  override def processRequest(request: BGetResultSetMetadataReq, done: RpcRequestClosure): Message = {
    val operationHandle = request.getOperationHandle
    val result = networkService.getResultSetMetadata(new handles.OperationHandle(operationHandle))
    BGetResultSetMetadataResp.newBuilder().setStatus(success()).setSchema(result.toBTableSchema()).build()
  }

  override def processError(rpcCtx: RpcContext, exception: Exception): Message = {
    BGetResultSetMetadataResp.newBuilder().setStatus(error(exception)).build()
  }

  override def interest(): String = classOf[BGetResultSetMetadataReq].getName

}
package org.bitlap.net.processor

import com.alipay.sofa.jraft.rpc.{ RpcContext, RpcRequestClosure }
import com.google.protobuf.Message
import org.bitlap.net.{ NetworkService, handles }
import org.bitlap.network.proto.driver.BFetchResults.{ BFetchResultsReq, BFetchResultsResp }

import java.util.concurrent.Executor

/**
 *
 * @author 梦境迷离
 * @since 2021/11/21
 * @version 1.0
 */
class FetchResultsProcessor(private val networkService: NetworkService, executor: Executor = null)
  extends BitlapRpcProcessor[BFetchResultsReq](executor, BFetchResultsResp.getDefaultInstance)
    with ProcessorHelper {

  override def processRequest(request: BFetchResultsReq, done: RpcRequestClosure): Message = {
    val operationHandle = request.getOperationHandle
    val result = networkService.fetchResults(new handles.OperationHandle(operationHandle))
    BFetchResultsResp.newBuilder()
      .setHasMoreRows(false)
      .setStatus(success()).setResults(result.toBRowSet()).build()
  }

  override def processError(rpcCtx: RpcContext, exception: Exception): Message = {
    BFetchResultsResp.newBuilder().setStatus(error(exception)).build()
  }

  override def interest(): String = classOf[BFetchResultsReq].getName
}

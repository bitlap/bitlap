package org.bitlap.net.processor

import com.alipay.sofa.jraft.rpc.{ RpcContext, RpcRequestClosure }
import com.google.protobuf.Message
import org.bitlap.net.{ NetworkService, handles }
import org.bitlap.network.proto.driver.BExecuteStatement.{ BExecuteStatementReq, BExecuteStatementResp }

import java.util.concurrent.Executor

/**
 *
 * @author 梦境迷离
 * @since 2021/11/21
 * @version 1.0
 */
class ExecuteStatementProcessor(private val networkService: NetworkService, executor: Executor = null)
  extends BitlapRpcProcessor[BExecuteStatementReq](executor, BExecuteStatementResp.getDefaultInstance)
    with ProcessorHelper {

  override def processRequest(request: BExecuteStatementReq, done: RpcRequestClosure): Message = {
    import scala.jdk.CollectionConverters.MapHasAsScala
    val sessionHandle = request.getSessionHandle
    val statement = request.getStatement
    val confOverlayMap = request.getConfOverlayMap.asScala.toMap
    val operationHandle = networkService.executeStatement(sessionHandle = new handles.SessionHandle(sessionHandle), statement = statement, confOverlay = confOverlayMap)
    BExecuteStatementResp.newBuilder()
      .setOperationHandle(operationHandle.toBOperationHandle())
      .setStatus(success()).build()
  }

  override def processError(rpcCtx: RpcContext, exception: Exception): Message = {
    BExecuteStatementResp.newBuilder().setStatus(error(exception)).build()
  }

  override def interest(): String = classOf[BExecuteStatementReq].getName

}

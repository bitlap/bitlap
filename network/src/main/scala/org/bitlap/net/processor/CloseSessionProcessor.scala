package org.bitlap.net.processor

import com.alipay.sofa.jraft.rpc.{ RpcContext, RpcRequestClosure }
import com.google.protobuf.Message
import org.bitlap.net.{ NetworkService, handles }
import org.bitlap.network.proto.driver.BCloseSession
import org.bitlap.network.proto.driver.BCloseSession.{ BCloseSessionReq, BCloseSessionResp }

import java.util.concurrent.Executor

/**
 *
 * @author 梦境迷离
 * @since 2021/11/21
 * @version 1.0
 */
class CloseSessionProcessor(private val networkService: NetworkService, executor: Executor = null)
  extends BitlapRpcProcessor[BCloseSession.BCloseSessionReq](executor, BCloseSessionResp.getDefaultInstance)
    with ProcessorHelper {

  override def processRequest(request: BCloseSessionReq, done: RpcRequestClosure): Message = {
    val sessionHandle = request.getSessionHandle
    networkService.closeSession(new handles.SessionHandle(sessionHandle))
    BCloseSessionResp.newBuilder().setStatus(success()).build()
  }

  override def processError(rpcCtx: RpcContext, exception: Exception): Message = {
    BCloseSessionResp.newBuilder().setStatus(error(exception)).build()
  }

  override def interest(): String = classOf[BCloseSessionReq].getName

}

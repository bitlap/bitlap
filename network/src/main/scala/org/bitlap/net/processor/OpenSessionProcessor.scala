package org.bitlap.net.processor

import com.alipay.sofa.jraft.rpc.{ RpcContext, RpcRequestClosure }
import com.google.protobuf.Message
import org.bitlap.net.NetworkService
import org.bitlap.network.proto.driver.BOpenSession.{ BOpenSessionReq, BOpenSessionResp }

import java.util.concurrent.Executor

/**
 *
 * @author 梦境迷离
 * @since 2021/11/21
 * @version 1.0
 */
class OpenSessionProcessor(private val networkService: NetworkService, executor: Executor = null)
  extends BitlapRpcProcessor[BOpenSessionReq](executor, BOpenSessionResp.getDefaultInstance) {

  override def processRequest(request: BOpenSessionReq, done: RpcRequestClosure): Message = {
    import scala.jdk.CollectionConverters.MapHasAsScala
    val username = request.getUsername
    val password = request.getPassword
    val configurationMap = request.getConfigurationMap
    val sessionHandle = networkService.openSession(username, password,
      configurationMap.asScala.toMap)
    BOpenSessionResp.newBuilder()
      .setSessionHandle(sessionHandle.toBSessionHandle())
      .setStatus(success()).build()
  }

  override def processError(rpcCtx: RpcContext, exception: Exception): Message = {
    BOpenSessionResp.newBuilder().setStatus(error(exception)).build()
  }

  override def interest(): String = classOf[BOpenSessionReq].getName
}
package org.bitlap.net.processor

import com.alipay.sofa.jraft.rpc.{ RpcContext, RpcRequestClosure, RpcRequestProcessor }
import com.google.protobuf.Message
import com.typesafe.scalalogging.LazyLogging

import java.util.concurrent.Executor

/**
 *
 * @author 梦境迷离
 * @since 2021/11/21
 * @version 1.0
 */
abstract class BitlapRpcProcessor[T <: Message](executor: Executor, override val defaultResp: Message)
  extends RpcRequestProcessor[T](executor, defaultResp)
    with ProcessorHelper with LazyLogging {

  override def handleRequest(rpcCtx: RpcContext, request: T) {
    try {
      val msg = processRequest(request, new RpcRequestClosure(rpcCtx, this.defaultResp))
      if (msg != null) {
        rpcCtx.sendResponse(msg)
      }
    } catch {
      case e: Exception =>
        logger.error(s"handleRequest $request failed", e)
        rpcCtx.sendResponse(processError(rpcCtx, e))
    }
  }

  def processError(rpcCtx: RpcContext, exception: Exception): Message
}

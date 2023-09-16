/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.server.raft.rpc

import java.util.concurrent.Executor

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcRequestClosure
import com.alipay.sofa.jraft.rpc.RpcRequestProcessor
import com.google.protobuf.Message

/** Writing abstract base classes for inheritance required by Raft RPC.
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/10/31
 */
abstract class RpcProcessor[T <: Message](executor: Executor = null, defaultResp: Message)
    extends RpcRequestProcessor[T](executor, defaultResp):

  override def handleRequest(rpcCtx: RpcContext, request: T): Unit =
    try {
      val msg = processRequest(request, new RpcRequestClosure(rpcCtx, this.defaultResp))
      if msg != null then {
        rpcCtx.sendResponse(msg)
      }
    } catch {
      case e: Exception => rpcCtx.sendResponse(processError(rpcCtx, e))

    }

  def processError(rpcCtx: RpcContext, exception: Exception): Message

  override def processRequest(request: T, done: RpcRequestClosure): Message

  override def interest(): String

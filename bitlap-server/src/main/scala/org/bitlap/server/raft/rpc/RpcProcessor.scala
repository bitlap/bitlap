/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.server.raft.rpc

import java.util.concurrent.Executor

import com.alipay.sofa.jraft.rpc.RpcContext
import com.alipay.sofa.jraft.rpc.RpcRequestClosure
import com.alipay.sofa.jraft.rpc.RpcRequestProcessor
import com.google.protobuf.Message

/** Writing abstract base classes for inheritance required by Raft RPC.
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

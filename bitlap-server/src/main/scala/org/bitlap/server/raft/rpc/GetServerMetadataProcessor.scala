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

import org.bitlap.common.BitlapConf
import org.bitlap.common.conf.BitlapConfKeys
import org.bitlap.network.{ GetServerAddressReq, GetServerAddressResp }
import org.bitlap.network.protocol.impl.*
import org.bitlap.server.config.BitlapConfiguration

import com.alipay.sofa.jraft.rpc.{ RpcProcessor as _, * }
import com.google.protobuf.Message

/** Use the RPC provided by Raft to obtain the metadata of the service itself, and provide it to
 *  [[org.bitlap.server.service.DriverGrpcService.getLeader()]].
 */
class GetServerMetadataProcessor(executor: Executor, conf: BitlapConf)
    extends RpcProcessor[GetServerAddressReq](
      executor,
      GetServerAddressResp.getDefaultInstance
    ):

  override def processRequest(request: GetServerAddressReq, done: RpcRequestClosure): Message = {
    val host    = conf.get(BitlapConfKeys.NODE_HOST).trim
    val address = host.asServerAddress
    val port    = address.port
    val ip      = address.ip
    GetServerAddressResp.newBuilder().setIp(ip).setPort(port).build()
  }

  override def processError(rpcCtx: RpcContext, exception: Exception): Message =
    GetServerAddressResp.newBuilder().setIp("").setPort(0).build()

  override def interest(): String = classOf[GetServerAddressReq].getName

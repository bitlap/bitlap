/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.raft.rpc

import org.bitlap.client.*

import com.alipay.sofa.jraft.rpc.{ RpcProcessor as _, * }
import com.google.protobuf.Message
import org.bitlap.common.schema.*
import org.bitlap.common.BitlapConf
import org.bitlap.server.BitlapContext
import java.util.concurrent.Executor
import org.bitlap.server.config.BitlapServerConfiguration

/** 使用raft提供的rpc来获取服务自身元数据，提供给[[org.bitlap.server.rpc.GrpcServiceLive.getLeader()]]使用
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/10/31
 */
class GetServerMetadataProcessor(executor: Executor = null)
    extends RpcProcessor[GetServerAddressReq](
      executor,
      GetServerAddressResp.getDefaultInstance
    ):

  override def processRequest(request: GetServerAddressReq, done: RpcRequestClosure): Message = {
    val host    = BitlapServerConfiguration.config.get(BitlapConf.NODE_BIND_HOST).trim
    val address = host.asServerAddress
    val port    = address.port
    val ip      = address.ip
    GetServerAddressResp.newBuilder().setIp(ip).setPort(port).build()
  }

  override def processError(rpcCtx: RpcContext, exception: Exception): Message =
    GetServerAddressResp.newBuilder().setIp("").setPort(0).build()

  override def interest(): String = classOf[GetServerAddressReq].getName

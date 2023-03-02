/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.raft.rpc

import com.alipay.sofa.jraft.rpc.{ RpcProcessor => _, _ }
import com.google.protobuf.Message
import org.bitlap.common.schema.GetServerMetadata
import org.bitlap.common.BitlapConf
import org.bitlap.server.BitlapServerContext
import java.util.concurrent.Executor
import org.bitlap.client._

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/31
 */
class GetServerMetadataProcessor(
  executor: Executor = null
) extends RpcProcessor[GetServerMetadata.GetServerAddressReq](
      executor,
      GetServerMetadata.GetServerAddressResp.getDefaultInstance
    ) {

  override def processRequest(request: GetServerMetadata.GetServerAddressReq, done: RpcRequestClosure): Message = {
    val host    = BitlapServerContext.globalConf.get(BitlapConf.NODE_BIND_HOST).trim
    val address = host.extractServerAddress
    val port    = address.port
    val ip      = address.ip
    GetServerMetadata.GetServerAddressResp.newBuilder().setIp(ip).setPort(port).build()
  }

  override def processError(rpcCtx: RpcContext, exception: Exception): Message =
    GetServerMetadata.GetServerAddressResp.newBuilder().setIp("").setPort(0).build()

  override def interest(): String = classOf[GetServerMetadata.GetServerAddressReq].getName
}

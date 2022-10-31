/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.raft.rpc

import com.alipay.sofa.jraft.rpc.{ RpcContext, RpcRequestClosure }
import com.google.protobuf.Message
import org.bitlap.common.schema.GetServerMetadata

import java.util.concurrent.Executor
import org.bitlap.common.BitlapConf
import scala.util.Try

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

  private lazy val conf = new BitlapConf()

  override def processRequest(request: GetServerMetadata.GetServerAddressReq, done: RpcRequestClosure): Message = {
    val host    = conf.get(BitlapConf.NODE_BIND_HOST).trim
    val address = if (host.contains(":")) host.split(":").toList.map(_.trim) else List(host, "23333")
    val ip      = address.head.trim
    val port    = Try(address(1).trim.toInt).getOrElse(23333)
    GetServerMetadata.GetServerAddressResp.newBuilder().setIp(ip).setPort(port).build()
  }

  override def processError(rpcCtx: RpcContext, exception: Exception): Message =
    GetServerMetadata.GetServerAddressResp.newBuilder().setIp("").setPort(0).build()

  override def interest(): String = classOf[GetServerMetadata.GetServerAddressReq].getName
}

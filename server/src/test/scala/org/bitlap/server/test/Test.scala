/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.test

import com.alipay.sofa.jraft.RouteTable
import com.alipay.sofa.jraft.rpc.RpcRequests.{ErrorResponse, PingRequest}
import org.bitlap.common.BitlapConf
import org.bitlap.network.RPC
import org.bitlap.server.BitlapServer

/**
 * @author 梦境迷离
 * @since 2021/6/12
 * @version 1.0
 */
object Test {

  def main(args: Array[String]): Unit = {
    val t = new Thread() {
      override def run(): Unit =
        BitlapServer.main(args)
    }
    t.start()

    Thread.sleep(5000L) // warm up

    // client
    val conf = new BitlapConf
    val peers = conf.get(BitlapConf.NODE_BIND_PEERS)
    val groupId = conf.get(BitlapConf.NODE_GROUP_ID)
    val timeout = conf.get(BitlapConf.NODE_RPC_TIMEOUT).toInt

    val client = RPC.newClient(peers, conf)
    assert(client.refreshLeader().isOk, "Refresh leader failed")
    val leader = RouteTable.getInstance().selectLeader(groupId)
    println(s"Leader is $leader")

    val req = PingRequest
      .newBuilder()
      .setSendTimestamp(System.currentTimeMillis())
      .build()
    val resp = client.invokeSync[ErrorResponse](req, timeout)
    println(s">>>>> ${resp.getErrorMsg}")

    t.interrupt()
    println("end....")
  }
}

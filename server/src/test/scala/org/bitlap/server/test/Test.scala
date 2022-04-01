/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.test

import com.alipay.sofa.jraft.RouteTable
import com.alipay.sofa.jraft.conf.Configuration
import com.alipay.sofa.jraft.option.CliOptions
import com.alipay.sofa.jraft.rpc.RpcRequests.PingRequest
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl
import org.bitlap.common.BitlapConf
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

    Thread.sleep(2000L) // warm up

    // client
    val conf = new BitlapConf
    val peers = conf.get(BitlapConf.NODE_BIND_PEERS)
    val groupId = conf.get(BitlapConf.NODE_GROUP_ID)
    val timeout = conf.get(BitlapConf.NODE_RAFT_TIMEOUT)
    val raftTimeout = if (timeout != null) timeout.toInt * 1000 else 1000

    val config = new Configuration()
    config.parse(peers)
    RouteTable.getInstance().updateConfiguration(groupId, config)
    val cli = new CliClientServiceImpl()
    cli.init(new CliOptions())
    assert(RouteTable.getInstance().refreshLeader(cli, groupId, raftTimeout).isOk, "Refresh leader failed")
    val leader = RouteTable.getInstance().selectLeader(groupId)
    println(s"Leader is $leader")

    val req = PingRequest.newBuilder().setSendTimestamp(System.currentTimeMillis()).build()
    val resp = cli.getRpcClient.invokeSync(leader.getEndpoint, req, 3000)
    println(s">>>>> ${resp.toString}")

    t.interrupt()
    println("end....")
  }
}

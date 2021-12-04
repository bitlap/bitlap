package org.bitlap.server

import org.bitlap.common.BitlapConf
import org.bitlap.server.raft.BitlapServerEndpoint

import com.alipay.sofa.jraft.conf.Configuration
import com.alipay.sofa.jraft.option.CliOptions
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl
import com.alipay.sofa.jraft.RouteTable

/**
 *
 * @author 梦境迷离
 * @version 1.0,2021/12/3
 */
class BitlapServer {

  val conf = new BitlapConf()
  private val server = new BitlapServerEndpoint(conf)

  def start() {
    Runtime.getRuntime.addShutdownHook(
      new Thread {
        if (server != null && !server.isShutdown) {
          server.close()
        }
      }
    )
    this.server.start()
  }
}

object BitlapServer extends App {

  val server = new BitlapServer()
  server.start()
  val groupId = server.conf.get(BitlapConf.getNODE_GROUP_ID.getGroup, BitlapConf.getNODE_GROUP_ID.getKey)
  val timeout = server.conf.get(BitlapConf.getNODE_RAFT_TIMEOUT.getGroup, BitlapConf.getNODE_RAFT_TIMEOUT.getKey)
  val raftTimeout = if (timeout != null) timeout.toInt * 1000 else 1000

  val conf = new Configuration()
  conf.parse("localhost:8001")
  RouteTable.getInstance().updateConfiguration(groupId, conf)
  val cli = new CliClientServiceImpl()
  cli.init(new CliOptions())
  assert(RouteTable.getInstance().refreshLeader(cli, groupId, raftTimeout).isOk, "Refresh leader failed")
  val leader = RouteTable.getInstance().selectLeader(groupId)
  println(s"Leader is $leader")

}

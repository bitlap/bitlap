/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.raft

import com.alipay.sofa.jraft._
import com.alipay.sofa.jraft.option._
import com.alipay.sofa.jraft.rpc.impl.cli._
import org.bitlap.network.LeaderAddress

import javax.annotation.Nullable

/** @since 2022/10/30
 *  @author
 *    梦境迷离
 */
object JRaftClient {

  val cliClientService: CliClientServiceImpl = new CliClientServiceImpl
  cliClientService.init(new CliOptions)

  @Nullable
  def getLeaderAddress(): LeaderAddress = {
    val rt      = RouteTable.getInstance
    val conf    = JRaftUtils.getConfiguration(RaftServerConfig.raftServerConfig.initialServerAddressList)
    val groupId = RaftServerConfig.raftServerConfig.groupId
    val timeout = RaftServerConfig.raftServerConfig.timeout
    rt.updateConfiguration(groupId, conf)
    val success: Boolean = rt.refreshLeader(cliClientService, groupId, timeout.toMillis.toInt).isOk
    val leader = if (success) {
      rt.selectLeader(groupId)
    } else null

    Option(leader).map(l => LeaderAddress(l.getIp, l.getPort)).orNull
  }

}

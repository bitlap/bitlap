/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.raft

import com.alipay.sofa.jraft._
import com.alipay.sofa.jraft.option._
import com.alipay.sofa.jraft.rpc.impl.cli._
import org.bitlap.network.LeaderAddress
import org.bitlap.network.NetworkException.ServerIntervalException

import javax.annotation.Nullable

/** @since 2022/10/30
 *  @author
 *    梦境迷离
 */
object RaftClient {

  private var cliClientService: CliClientServiceImpl = _

  def init(): Boolean = {
    cliClientService = new CliClientServiceImpl
    cliClientService.init(new CliOptions)
  }

  @Nullable
  def getLeaderAddress(): LeaderAddress = {
    if (cliClientService == null) {
      throw ServerIntervalException("cannot find raft CLI service")
    }
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

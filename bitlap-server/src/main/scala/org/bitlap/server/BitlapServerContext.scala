/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server

import com.alipay.sofa.jraft.option.CliOptions
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl
import com.alipay.sofa.jraft.{ JRaftUtils, Node, RouteTable }
import org.bitlap.network.LeaderAddress
import org.bitlap.network.NetworkException.ServerIntervalException
import org.bitlap.server.raft.RaftServerConfig
import java.util.concurrent.atomic.AtomicBoolean

import javax.annotation.Nullable

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/31
 */
object BitlapServerContext {

  private val inited = new AtomicBoolean(false)

  @volatile
  private var cliClientService: CliClientServiceImpl = _

  @volatile
  private var _node: Node = _

  def init(node: Node): Unit =
    if (inited.compareAndSet(false, true)) {
      _node = node
      cliClientService = new CliClientServiceImpl
      cliClientService.init(new CliOptions)
    }

  def isLeader: Boolean = _node.isLeader

  @Nullable
  def getLeaderAddress(): LeaderAddress =
    if (isLeader) {
      if (_node == null) {
        throw ServerIntervalException("cannot find a raft node instance")
      }
      Option(_node.getLeaderId).map(l => LeaderAddress(l.getIp, l.getPort)).orNull
    } else {
      if (cliClientService == null) {
        throw ServerIntervalException("cannot find a raft CliClientService instance")
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
      // RPC
    }

}

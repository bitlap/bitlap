/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server

import com.alipay.sofa.jraft.{ JRaftUtils, Node, RouteTable }
import com.alipay.sofa.jraft.option.CliOptions
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl
import org.bitlap.common.schema.BGetServerMetadata
import org.bitlap.common.utils.UuidUtil
import org.bitlap.network.LeaderAddress
import org.bitlap.network.NetworkException.LeaderServerNotFoundException
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
        throw LeaderServerNotFoundException("cannot find a raft node instance")
      }
      Option(_node.getLeaderId).map(l => LeaderAddress(l.getIp, l.getPort)).orNull
    } else {
      if (cliClientService == null) {
        throw LeaderServerNotFoundException("cannot find a raft CliClientService instance")
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
      if (leader == null) {
        throw LeaderServerNotFoundException("cannot select a leader")
      }
      val result = cliClientService.getRpcClient.invokeSync(
        leader.getEndpoint,
        BGetServerMetadata.BGetServerAddressReq
          .newBuilder()
          .setRequestId(UuidUtil.uuid())
          .build(),
        timeout.toMillis
      )
      val re = result.asInstanceOf[BGetServerMetadata.BGetServerAddressResp]

      if (re == null || re.getIp.isEmpty || re.getPort <= 0)
        throw LeaderServerNotFoundException("cannot find a leader address")
      else LeaderAddress(re.getIp, re.getPort)
    }

}

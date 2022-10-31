/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server

import com.alipay.sofa.jraft.{ JRaftUtils, Node, RouteTable }
import com.alipay.sofa.jraft.option.CliOptions
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl
import org.bitlap.common.utils.UuidUtil
import org.bitlap.common.BitlapConf
import org.bitlap.common.schema.GetServerMetadata
import org.bitlap.network.LeaderGrpcAddress
import org.bitlap.network.NetworkException.LeaderServerNotFoundException
import org.bitlap.server.raft.RaftServerConfig

import java.util.concurrent.atomic.AtomicBoolean
import javax.annotation.Nullable
import org.bitlap.network.AsyncRpc
import zio._
import org.bitlap.network.NetworkException.ServerIntervalException

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/31
 */
object BitlapServerContext {

  private val initNode = new AtomicBoolean(false)
  private val initRpc  = new AtomicBoolean(false)

  @volatile
  private var cliClientService: CliClientServiceImpl = _

  @volatile
  private var _asyncRpc: AsyncRpc = _

  @volatile
  private var _node: Node = _

  def asyncRpc: AsyncRpc =
    if (_asyncRpc == null) {
      throw ServerIntervalException("cannot find an AsyncRpc instance")
    } else {
      _asyncRpc
    }

  def fillRpc(asyncRpc: AsyncRpc): UIO[Unit] = ZIO.succeed {
    if (initRpc.compareAndSet(false, true)) {
      _asyncRpc = asyncRpc
    }
  }

  def fillNode(node: Node): Task[Unit] =
    zio.blocking.blocking {
      ZIO.effect {
        if (initNode.compareAndSet(false, true)) {
          _node = node
          cliClientService = new CliClientServiceImpl
          cliClientService.init(new CliOptions)
        }
        ()
      }
    }.provideLayer(zio.blocking.Blocking.live)

  def isLeader: Boolean = _node.isLeader

  @Nullable
  def getLeaderAddress(): Task[LeaderGrpcAddress] = Task.effect {
    if (isLeader) {
      if (_node == null) {
        throw LeaderServerNotFoundException("cannot find a raft node instance")
      }
      Option(_node.getLeaderId).map(l => LeaderGrpcAddress(l.getIp, grpcServerPort)).orNull
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
        GetServerMetadata.GetServerAddressReq
          .newBuilder()
          .setRequestId(UuidUtil.uuid())
          .build(),
        timeout.toMillis
      )
      val re = result.asInstanceOf[GetServerMetadata.GetServerAddressResp]

      if (re == null || re.getIp.isEmpty || re.getPort <= 0)
        throw LeaderServerNotFoundException("cannot find a leader address")
      else LeaderGrpcAddress(re.getIp, re.getPort)
    }
  }

  private def grpcServerPort: Int = {
    lazy val conf = new BitlapConf()
    val address   = conf.get(BitlapConf.NODE_BIND_HOST).trim
    val ipPorts   = if (address.contains(":")) address.split(":").toList.map(_.trim) else List(address, "23333")
    ipPorts(1).toIntOption.getOrElse(23333)
  }
}

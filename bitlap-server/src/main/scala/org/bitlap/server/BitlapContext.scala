/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server

import com.alipay.sofa.jraft._
import com.alipay.sofa.jraft.option.CliOptions
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl
import org.bitlap.client._
import org.bitlap.common.BitlapConf
import org.bitlap.common.schema._
import org.bitlap.common.utils.UuidUtil
import org.bitlap.network.NetworkException._
import org.bitlap.network.{ DriverAsyncRpc, ServerAddress }
import org.bitlap.server.config.BitlapRaftConfig
import zio._

import java.util.concurrent.atomic.AtomicBoolean
import javax.annotation.Nullable

/** bitlap 服务间上下文，用于grpc,http,raft数据依赖
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/10/31
 */
object BitlapContext {

  lazy val globalConf = new BitlapConf()

  private val initNode = new AtomicBoolean(false)
  private val initRpc  = new AtomicBoolean(false)

  @volatile
  private var cliClientService: CliClientServiceImpl = _

  @volatile
  private var _asyncRpc: DriverAsyncRpc = _

  @volatile
  private var _node: Node = _

  def asyncRpc: DriverAsyncRpc =
    if (_asyncRpc == null) {
      throw InternalException("cannot find an AsyncRpc instance")
    } else {
      _asyncRpc
    }

  def fillRpc(asyncRpc: DriverAsyncRpc): UIO[Unit] =
    ZIO.succeed {
      if (initRpc.compareAndSet(false, true)) {
        _asyncRpc = asyncRpc
      }
    }

  def fillNode(node: Node): Task[Unit] =
    ZIO.attemptBlocking {
      if (initNode.compareAndSet(false, true)) {
        _node = node
        cliClientService = new CliClientServiceImpl
        cliClientService.init(new CliOptions)
      }
      ()
    }

  def isLeader: Boolean = {
    while (_node == null)
      Thread.sleep(1000)
    _node.isLeader
  }

  @Nullable
  def getLeaderAddress(): ZIO[Any, Throwable, ServerAddress] =
    (for {
      conf    <- ZIO.serviceWith[BitlapRaftConfig](c => c.initialServerAddressList)
      groupId <- ZIO.serviceWith[BitlapRaftConfig](c => c.groupId)
      timeout <- ZIO.serviceWith[BitlapRaftConfig](c => c.timeout)
      server <- ZIO.attempt {
        if (isLeader) {
          if (_node == null) {
            throw LeaderNotFoundException("cannot find a raft node instance")
          }

          def grpcServerPort: Int = {
            val address = globalConf.get(BitlapConf.NODE_BIND_HOST).trim
            address.asServerAddress.port
          }
          Option(_node.getLeaderId).map(l => ServerAddress(l.getIp, grpcServerPort)).orNull
        } else {
          if (cliClientService == null) {
            throw LeaderNotFoundException("cannot find a raft CliClientService instance")
          }
          val rt = RouteTable.getInstance
          rt.updateConfiguration(groupId, conf)
          val success: Boolean = rt.refreshLeader(cliClientService, groupId, timeout.toMillis.toInt).isOk
          val leader = if (success) {
            rt.selectLeader(groupId)
          } else null
          if (leader == null) {
            throw LeaderNotFoundException("cannot select a leader")
          }
          val result = cliClientService.getRpcClient.invokeSync(
            leader.getEndpoint,
            GetServerAddressReq
              .newBuilder()
              .setRequestId(UuidUtil.uuid())
              .build(),
            timeout.toMillis
          )
          val re = result.asInstanceOf[GetServerAddressResp]

          if (re == null || re.getIp.isEmpty || re.getPort <= 0)
            throw LeaderNotFoundException("cannot find a leader address")
          else ServerAddress(re.getIp, re.getPort)
        }
      }
    } yield server).provideLayer(BitlapRaftConfig.live)

}

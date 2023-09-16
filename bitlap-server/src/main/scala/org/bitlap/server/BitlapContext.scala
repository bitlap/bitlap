/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.server

import java.util.concurrent.atomic.AtomicBoolean
import javax.annotation.Nullable

import org.bitlap.client.*
import org.bitlap.common.BitlapConf
import org.bitlap.common.utils.StringEx
import org.bitlap.network.{ DriverIO, GetServerAddressReq, GetServerAddressResp, ServerAddress }
import org.bitlap.network.NetworkException.*
import org.bitlap.server.config.*

import com.alipay.sofa.jraft.*
import com.alipay.sofa.jraft.option.CliOptions
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl

import zio.*

/** Bitlap inter service context for GRPC, HTTP, Raft data dependencies
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/10/31
 */
object BitlapContext:

  lazy val globalConf: BitlapConf = org.bitlap.core.BitlapContext.INSTANCE.getBitlapConf

  private val initNode = new AtomicBoolean(false)
  private val initRpc  = new AtomicBoolean(false)

  @volatile
  private var cliClientService: CliClientServiceImpl = _

  @volatile
  private var _driverIO: DriverIO = _

  @volatile
  private var _node: Node = _

  def driverIO: DriverIO =
    if _driverIO == null then {
      throw InternalException("cannot find a DriverIO instance")
    } else {
      _driverIO
    }

  def fillRpc(driverIO: DriverIO): UIO[Unit] =
    ZIO.succeed {
      if initRpc.compareAndSet(false, true) then {
        _driverIO = driverIO
      }
    }

  def fillNode(node: Node): Task[Unit] =
    ZIO.attemptBlocking {
      if initNode.compareAndSet(false, true) then {
        _node = node
        cliClientService = new CliClientServiceImpl
        cliClientService.init(new CliOptions)
      }
      ()
    }

  def isLeader: Boolean = {
    while (_node == null) {
      Thread.sleep(1000)
    }
    _node.isLeader
  }

  @Nullable
  def getLeaderAddress(): ZIO[Any, Throwable, ServerAddress] =
    (for {
      conf       <- ZIO.serviceWith[BitlapServerConfiguration](c => c.raftConfig.initialServerAddressList)
      groupId    <- ZIO.serviceWith[BitlapServerConfiguration](c => c.raftConfig.groupId)
      timeout    <- ZIO.serviceWith[BitlapServerConfiguration](c => c.raftConfig.timeout)
      grpcConfig <- ZIO.serviceWith[BitlapServerConfiguration](c => c.grpcConfig)
      server <- ZIO.attempt {
        if isLeader then {
          if _node == null then {
            throw LeaderNotFoundException("cannot find a raft node instance")
          }
          Option(_node.getLeaderId).map(l => ServerAddress(l.getIp, grpcConfig.port)).orNull
        } else {
          if cliClientService == null then {
            throw LeaderNotFoundException("cannot find a raft CliClientService instance")
          }
          val rt = RouteTable.getInstance
          rt.updateConfiguration(groupId, conf)
          val success: Boolean = rt.refreshLeader(cliClientService, groupId, timeout.toMillis.toInt).isOk
          val leader = if success then {
            rt.selectLeader(groupId)
          } else null
          if leader == null then {
            throw LeaderNotFoundException("cannot select a leader")
          }
          val result = cliClientService.getRpcClient.invokeSync(
            leader.getEndpoint,
            GetServerAddressReq
              .newBuilder()
              .setRequestId(StringEx.uuid(true))
              .build(),
            timeout.toMillis
          )
          val re = result.asInstanceOf[GetServerAddressResp]

          if re == null || re.getIp.isEmpty || re.getPort <= 0 then
            throw LeaderNotFoundException("cannot find a leader address")
          else ServerAddress(re.getIp, re.getPort)
        }
      }
    } yield server).provideLayer(BitlapServerConfiguration.live)

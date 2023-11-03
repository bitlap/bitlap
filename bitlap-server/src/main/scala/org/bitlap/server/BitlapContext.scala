/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.server

import java.util.concurrent.atomic.AtomicBoolean
import javax.annotation.Nullable

import org.bitlap.client.*
import org.bitlap.common.BitlapConf
import org.bitlap.common.utils.StringEx
import org.bitlap.network.{ AsyncProtocol, GetServerAddressReq, GetServerAddressResp, NetworkException, ServerAddress }
import org.bitlap.network.NetworkException.*
import org.bitlap.server.BitlapContext.cliClientServiceRef
import org.bitlap.server.config.*

import com.alipay.sofa.jraft.*
import com.alipay.sofa.jraft.option.CliOptions
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl

import zio.*

/** Bitlap inter service context for GRPC, HTTP, Raft data dependencies
 */
object BitlapContext:

  lazy val globalConf: BitlapConf = org.bitlap.core.BitlapContext.bitlapConf

  private val initNode = new AtomicBoolean(false)
  private val initRpc  = new AtomicBoolean(false)

  @volatile
  private val cliClientServiceRef: UIO[Ref[CliClientServiceImpl]] = Ref.make(new CliClientServiceImpl)

  @volatile
  private var _protocolRef: AsyncProtocol = _

  @volatile
  private var _node: Node = _

  def protocolRef: ZIO[Any, NetworkException, AsyncProtocol] =
    if _protocolRef == null then {
      ZIO.fail(InternalException("cannot find a AsyncProtocol instance"))
    } else {
      ZIO.succeed(_protocolRef)
    }

  def fillRpc(_asyncProtocol: AsyncProtocol): UIO[Unit] =
    ZIO.attemptBlocking {
      if initRpc.compareAndSet(false, true) then {
        _protocolRef = _asyncProtocol
      }
    }.ignoreLogged.unit

  def fillNode(node: Node): Task[Unit] =
    cliClientServiceRef.flatMap { ref =>
      ZIO
        .when(initNode.compareAndSet(false, true)) {
          _node = node
          ref.get.map(_.init(new CliOptions)).unit
        }
        .unit
    }

  def isLeader: ZIO[Any, Throwable, Boolean] = {
    for {
      timeout <- ZIO
        .serviceWith[BitlapServerConfiguration](c => c.raftConfig.timeout)
        .provideLayer(BitlapServerConfiguration.live)
      res <- ZIO.attempt {
        while (_node == null) {
          Thread.sleep(1000)
        }
        _node.isLeader
      }.timeout(Duration.fromScala(timeout))
        .someOrFail(
          InternalException("leader's preparation failed")
        )
    } yield res

  }

  @Nullable
  def getLeaderAddress(): ZIO[Any, Throwable, ServerAddress] =
    (for {
      conf              <- ZIO.serviceWith[BitlapServerConfiguration](c => c.raftConfig.initialServerAddressList)
      groupId           <- ZIO.serviceWith[BitlapServerConfiguration](c => c.raftConfig.groupId)
      timeout           <- ZIO.serviceWith[BitlapServerConfiguration](c => c.raftConfig.timeout)
      grpcConfig        <- ZIO.serviceWith[BitlapServerConfiguration](c => c.grpcConfig)
      _cliClientService <- cliClientServiceRef
      cliClientService  <- _cliClientService.get
      server <- ZIO
        .whenZIO(isLeader) {
          ZIO.succeed {
            Option(_node.getLeaderId).map(l => ServerAddress(l.getIp, grpcConfig.port)).orNull
          }
        }
        .someOrElse {
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
    } yield server).provideLayer(BitlapServerConfiguration.live)

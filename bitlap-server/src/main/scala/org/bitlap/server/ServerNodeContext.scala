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
import org.bitlap.network.*
import org.bitlap.network.NetworkException.*
import org.bitlap.server.config.*

import com.alipay.sofa.jraft.*
import com.alipay.sofa.jraft.option.CliOptions
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl

import zio.*

/** Bitlap inter service context for GRPC, HTTP, Raft data dependencies
 */
final case class ServerNodeContext(
  config: BitlapServerConfiguration,
  grpcStarted: Promise[Throwable, Boolean],
  raftStarted: Promise[Throwable, Boolean],
  cliClientServiceRef: Ref[CliClientServiceImpl],
  nodeRef: Ref[Option[Node]],
  clientRef: Ref[Option[AsyncClient]]) {

  private val refTimeout = Duration.fromScala(config.startTimeout) // require a timeout?

  def start(): ZIO[Any, Throwable, Boolean] = grpcStarted.succeed(true) && raftStarted.succeed(true)

  def isStarted: ZIO[Any, Throwable, Boolean] = grpcStarted.await *> raftStarted.await

  def setClient(client: AsyncClient): ZIO[Any, Throwable, Unit] =
    grpcStarted.await.timeout(refTimeout) *> clientRef.set(Option(client))

  def getClient: ZIO[Any, Throwable, AsyncClient] =
    grpcStarted.await.timeout(refTimeout) *>
      clientRef.get.someOrFail(
        InternalException("Cannot find a AsyncClient instance")
      )

  def getNode: ZIO[Any, Throwable, Node] =
    raftStarted.await.timeout(refTimeout) *> nodeRef.get.someOrFail(
      InternalException("Cannot find a Node instance")
    )

  def getCliClientService: UIO[CliClientServiceImpl] = cliClientServiceRef.get

  def setNode(_node: Node): ZIO[Any, Throwable, Boolean] =
    for {
      _   <- nodeRef.set(Option(_node))
      cli <- cliClientServiceRef.get
      r <- ZIO
        .attemptBlocking(cli.init(new CliOptions))
        .mapError(e => InternalException("Failed to initialize CliClientService", cause = Option(e)))
    } yield r

  def isLeader: ZIO[Any, Throwable, Boolean] =
    isStarted *> nodeRef.get.someOrFail(LeaderNotFoundException("Cannot find a leader")).map(_.isLeader)

  def getLeaderAddress(): Task[ServerAddress] =
    (for {
      config <- ZIO.service[BitlapServerConfiguration]
      peers      = config.raftConfig.initialServerAddressList
      groupId    = config.raftConfig.groupId
      timeout    = config.raftConfig.timeout
      grpcConfig = config.grpcConfig
      cliClientService <- cliClientServiceRef.get
      _node            <- nodeRef.get
      server <- ZIO
        .whenZIO(isLeader) {
          ZIO.succeed {
            _node.map(_.getLeaderId).map(l => ServerAddress(l.getIp, grpcConfig.port)).orNull
          }
        }
        .someOrElse {
          val rt = RouteTable.getInstance
          rt.updateConfiguration(groupId, peers)
          val success: Boolean = rt.refreshLeader(cliClientService, groupId, timeout.toMillis.toInt).isOk
          val leader = if success then {
            rt.selectLeader(groupId)
          } else null
          if leader == null then {
            throw LeaderNotFoundException("Cannot select a leader")
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
            throw LeaderNotFoundException("Cannot find a leader address")
          else ServerAddress(re.getIp, re.getPort)
        }
    } yield server).provideLayer(BitlapServerConfiguration.live)
}

object ServerNodeContext:

  lazy val live: ZLayer[BitlapServerConfiguration, Nothing, ServerNodeContext] = ZLayer.fromZIO {
    for {
      grpcStart        <- Promise.make[Throwable, Boolean]
      raftStart        <- Promise.make[Throwable, Boolean]
      cliClientService <- Ref.make(new CliClientServiceImpl)
      node             <- Ref.make(Option.empty[Node])
      client           <- Ref.make(Option.empty[AsyncClient])
      config           <- ZIO.service[BitlapServerConfiguration]
    } yield ServerNodeContext(config, grpcStart, raftStart, cliClientService, node, client)
  }

end ServerNodeContext

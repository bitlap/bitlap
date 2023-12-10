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

import org.bitlap.common.exception.*
import org.bitlap.common.utils.StringEx
import org.bitlap.network.*
import org.bitlap.server.config.*
import org.bitlap.server.session.*

import com.alipay.sofa.jraft.*
import com.alipay.sofa.jraft.option.CliOptions
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl

import zio.*

/** Bitlap inter service context for GRPC, HTTP, Raft data dependencies
 */
final class BitlapGlobalContext(
  val config: BitlapConfigWrapper,
  grpcStarted: Promise[Throwable, Boolean],
  raftStarted: Promise[Throwable, Boolean],
  cliClientServiceRef: Ref[CliClientServiceImpl],
  nodeRef: Ref[Option[Node]],
  syncConnectionRef: Ref[Option[SyncConnection]],
  sessionManagerRef: Ref[Option[SessionManager]]) {

  private val refTimeout = Duration.fromScala(config.startTimeout) // require a timeout?

  def startFinished(): ZIO[Any, Throwable, Boolean] = {
    grpcStarted.succeed(true) && raftStarted.succeed(true)
  }

  def isStarted: ZIO[Any, Throwable, Boolean] = grpcStarted.await *> raftStarted.await

  def getSessionManager: ZIO[Any, Throwable, SessionManager] = grpcStarted.await.timeout(refTimeout) *>
    sessionManagerRef.get.someOrFail(BitlapException("Cannot find a SessionManager instance"))

  def setSessionManager(sessionManager: SessionManager): ZIO[Any, Throwable, Unit] =
    grpcStarted.await.timeout(refTimeout) *> sessionManagerRef.set(Option(sessionManager))

  def setSyncConnection(syncConnection: SyncConnection): ZIO[Any, Throwable, Unit] =
    grpcStarted.await.timeout(refTimeout) *> syncConnectionRef.set(Option(syncConnection))

  def getSyncConnection: ZIO[Any, Throwable, SyncConnection] =
    grpcStarted.await.timeout(refTimeout) *>
      syncConnectionRef.get.someOrFail(BitlapException("Cannot find a SyncConnection instance"))

  def getNode: ZIO[Any, Throwable, Node] =
    raftStarted.await.timeout(refTimeout) *> nodeRef.get.someOrFail(BitlapException("Cannot find a Node instance"))

  def getCliClientService: UIO[CliClientServiceImpl] = cliClientServiceRef.get

  def setNode(_node: Node): ZIO[Any, Throwable, Boolean] =
    for {
      _   <- nodeRef.set(Option(_node))
      cli <- cliClientServiceRef.get
      r <- ZIO
        .attemptBlocking(cli.init(new CliOptions))
        .mapError(e => BitlapException("Failed to initialize CliClientService", cause = Option(e)))
    } yield r

  def isLeader: ZIO[Any, Throwable, Boolean] =
    isStarted *> nodeRef.get.someOrFail(BitlapIllegalStateException("Cannot find a leader")).map(_.isLeader)

  def getLeaderOrRefresh(): Task[ServerAddress] =
    for {
      cliClientService <- cliClientServiceRef.get
      peers      = config.raftConfig.initialServerAddressList
      groupId    = config.raftConfig.groupId
      timeout    = config.raftConfig.timeout
      grpcConfig = config.grpcConfig
      _node <- nodeRef.get
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
            throw BitlapIllegalStateException("Cannot select a leader")
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
            throw BitlapIllegalStateException("Cannot find a leader address")
          else ServerAddress(re.getIp, re.getPort)
        }
    } yield server
}

object BitlapGlobalContext:

  lazy val live: ZLayer[BitlapConfigWrapper, Nothing, BitlapGlobalContext] = ZLayer.fromZIO {
    for {
      grpcStart        <- Promise.make[Throwable, Boolean]
      raftStart        <- Promise.make[Throwable, Boolean]
      cliClientService <- Ref.make(new CliClientServiceImpl)
      node             <- Ref.make(Option.empty[Node])
      syncConnection   <- Ref.make(Option.empty[SyncConnection])
      config           <- ZIO.service[BitlapConfigWrapper]
      sessionManager   <- Ref.make(Option.empty[SessionManager])
    } yield BitlapGlobalContext(
      config,
      grpcStart,
      raftStart,
      cliClientService,
      node,
      syncConnection,
      sessionManager
    )
  }

end BitlapGlobalContext

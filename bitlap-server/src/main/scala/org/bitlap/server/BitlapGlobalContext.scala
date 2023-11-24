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

import java.util.Vector as JVector
import java.util.concurrent.ConcurrentHashMap

import org.bitlap.common.exception.{ BitlapException, BitlapIllegalStateException }
import org.bitlap.common.utils.StringEx
import org.bitlap.network.*
import org.bitlap.network.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.protocol.impl.*
import org.bitlap.server.config.*
import org.bitlap.server.session.{ Operation, Session }

import com.alipay.sofa.jraft.*
import com.alipay.sofa.jraft.option.CliOptions
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl

import zio.*

/** Bitlap inter service context for GRPC, HTTP, Raft data dependencies
 */
final case class BitlapGlobalContext(
  config: BitlapConfiguration,
  grpcStarted: Promise[Throwable, Boolean],
  raftStarted: Promise[Throwable, Boolean],
  cliClientServiceRef: Ref[CliClientServiceImpl],
  nodeRef: Ref[Option[Node]],
  asyncRef: Ref[Option[Async]],
  sessionStoreMap: Ref[ConcurrentHashMap[SessionHandle, Session]],
  operationHandleVector: Ref[JVector[OperationHandle]],
  operationStoreMap: Ref[ConcurrentHashMap[OperationHandle, Operation]]) {

  private val refTimeout = Duration.fromScala(config.startTimeout) // require a timeout?

  def start(): ZIO[Any, Throwable, Boolean] = grpcStarted.succeed(true) && raftStarted.succeed(true)

  def isStarted: ZIO[Any, Throwable, Boolean] = grpcStarted.await *> raftStarted.await

  def setProtocolImpl(async: Async): ZIO[Any, Throwable, Unit] =
    grpcStarted.await.timeout(refTimeout) *> asyncRef.set(Option(async))

  def getClient: ZIO[Any, Throwable, Async] =
    grpcStarted.await.timeout(refTimeout) *>
      asyncRef.get.someOrFail(
        BitlapException("Cannot find a Async instance")
      )

  def getNode: ZIO[Any, Throwable, Node] =
    raftStarted.await.timeout(refTimeout) *> nodeRef.get.someOrFail(
      BitlapException("Cannot find a Node instance")
    )

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

  def getLeaderAddress(): Task[ServerAddress] =
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

  lazy val live: ZLayer[BitlapConfiguration, Nothing, BitlapGlobalContext] = ZLayer.fromZIO {
    for {
      grpcStart             <- Promise.make[Throwable, Boolean]
      raftStart             <- Promise.make[Throwable, Boolean]
      cliClientService      <- Ref.make(new CliClientServiceImpl)
      node                  <- Ref.make(Option.empty[Node])
      async                 <- Ref.make(Option.empty[Async])
      config                <- ZIO.service[BitlapConfiguration]
      SessionStoreMap       <- Ref.make(ConcurrentHashMap[SessionHandle, Session]())
      OperationHandleVector <- Ref.make(JVector[OperationHandle]())
      OperationStoreMap     <- Ref.make(ConcurrentHashMap[OperationHandle, Operation]())
    } yield BitlapGlobalContext(
      config,
      grpcStart,
      raftStart,
      cliClientService,
      node,
      async,
      SessionStoreMap,
      OperationHandleVector,
      OperationStoreMap
    )
  }

end BitlapGlobalContext

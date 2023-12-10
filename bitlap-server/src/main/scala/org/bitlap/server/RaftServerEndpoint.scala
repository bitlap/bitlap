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

import org.bitlap.common.BitlapLogging
import org.bitlap.common.exception.BitlapException
import org.bitlap.server.config.BitlapConfigWrapper
import org.bitlap.server.raft.*

import com.alipay.sofa.jraft.Node
import com.alipay.sofa.jraft.option.NodeOptions

import zio.{ Runtime as _, * }

/** Bitlap Raft cluster and RPC service
 */
object RaftServerEndpoint {

  val live: ZLayer[BitlapConfigWrapper, Nothing, RaftServerEndpoint] =
    ZLayer.fromFunction((conf: BitlapConfigWrapper) => new RaftServerEndpoint(conf))

  def service(args: List[String])
    : ZIO[RaftServerEndpoint & BitlapGlobalContext & BitlapConfigWrapper, Throwable, Unit] =
    (for {
      node  <- ZIO.serviceWithZIO[RaftServerEndpoint](_.runRaftServer())
      _     <- ZIO.serviceWithZIO[BitlapGlobalContext](_.setNode(node))
      ports <- ZIO.serviceWith[BitlapConfigWrapper](_.raftConfig.getPorts)
      _     <- ZIO.logInfo(s"Raft Server started at ports: ${ports.mkString(",")}")
      _     <- ZIO.never
    } yield ())
      .onInterrupt(_ => ZIO.logWarning(s"Raft Server was interrupted! Bye!"))
}

final class RaftServerEndpoint(config: BitlapConfigWrapper) extends BitlapLogging {

  private val dataPath       = config.raftConfig.dataPath
  private val groupId        = config.raftConfig.groupId
  private val serverIdStr    = config.raftConfig.serverAddress
  private val initialConfStr = config.raftConfig.initialServerAddressList

  private def runRaftServer(): Task[Node] =
    for {
      promise <- Promise.make[Throwable, ElectionNode]
      _ <- promise.complete(ZIO.attempt {
        val electionOpts = ElectionNodeOptions(
          dataPath = dataPath,
          groupId = groupId,
          serverAddress = serverIdStr,
          initialServerAddressList = initialConfStr,
          new NodeOptions
        )
        val node = new ElectionNode
        node.addLeaderStateListener(new LeaderStateListener() {
          override def onLeaderStart(leaderTerm: Long): Unit = {
            log.info(s"[ElectionBootstrap] Leader's address is: $serverIdStr")
            log.info(s"[ElectionBootstrap] Leader start on term: $leaderTerm")
          }

          override def onLeaderStop(leaderTerm: Long): Unit =
            log.info(s"[ElectionBootstrap] Leader stop on term: $leaderTerm")
        })

        Runtime.getRuntime.addShutdownHook(new Thread(() => node.shutdown()))
        node.init(electionOpts)
        node
      })
      node <- promise.await
        .timeout(Duration.fromScala(config.startTimeout))
        .someOrFail(
          BitlapException("Failed to initialize raft node")
        )
    } yield node.node
}

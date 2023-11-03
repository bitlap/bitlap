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

import org.bitlap.server.config.BitlapRaftConfig
import org.bitlap.server.config.BitlapServerConfiguration
import org.bitlap.server.raft.*
import org.bitlap.server.service.DriverService

import org.slf4j.LoggerFactory

import com.alipay.sofa.jraft.Node
import com.alipay.sofa.jraft.option.NodeOptions

import zio.{ Runtime as _, * }

/** Bitlap Raft cluster and RPC service
 */
object RaftServerEndpoint:

  lazy val live: ZLayer[BitlapServerConfiguration, Nothing, RaftServerEndpoint] =
    ZLayer.fromFunction((conf: BitlapServerConfiguration) => new RaftServerEndpoint(conf))

  def service(args: List[String]): ZIO[RaftServerEndpoint with ServerNodeContext, Throwable, Unit] =
    (for {
      node <- ZIO.serviceWithZIO[RaftServerEndpoint](_.runRaftServer())
      _    <- ZIO.serviceWithZIO[ServerNodeContext](_.setNode(node))
      _    <- Console.printLine(s"Raft Server started")
      _    <- ZIO.never
    } yield ())
      .onInterrupt(_ => Console.printLine(s"Raft Server was interrupted").ignore)
end RaftServerEndpoint

final class RaftServerEndpoint(config: BitlapServerConfiguration):

  private lazy val LOG = LoggerFactory.getLogger(classOf[ElectionOnlyStateMachine])

  private def runRaftServer(): Task[Node] = ZIO.attempt {
    val dataPath       = config.raftConfig.dataPath
    val groupId        = config.raftConfig.groupId
    val serverIdStr    = config.raftConfig.serverAddress
    val initialConfStr = config.raftConfig.initialServerAddressList

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
        LOG.info(s"[ElectionBootstrap] Leader's address is: $serverIdStr")
        LOG.info(s"[ElectionBootstrap] Leader start on term: $leaderTerm")
      }

      override def onLeaderStop(leaderTerm: Long): Unit =
        LOG.info(s"[ElectionBootstrap] Leader stop on term: $leaderTerm")
    })

    Runtime.getRuntime.addShutdownHook(new Thread(() => node.shutdown()))

    node.init(electionOpts)

    while node.node == null do Thread.sleep(1000)
    node.node
  }

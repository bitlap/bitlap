/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server

import com.alipay.sofa.jraft.Node
import com.alipay.sofa.jraft.option.NodeOptions
import org.bitlap.server.config.BitlapRaftConfig
import org.bitlap.server.raft._
import org.slf4j.LoggerFactory
import zio.blocking.Blocking
import zio.console.{ putStrLn, Console }
import zio.{ Runtime => _, _ }

/** bitlap raft cluster和rpc服务
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/10/28
 */
object RaftServerEndpoint {

  lazy val live: ZLayer[Has[BitlapRaftConfig], Nothing, Has[RaftServerEndpoint]] =
    ZLayer.fromService((conf: BitlapRaftConfig) => new RaftServerEndpoint(conf))

  def service(args: List[String]): ZIO[Console with Blocking with Has[RaftServerEndpoint], Throwable, Unit] =
    (for {
      node <- ZIO.serviceWith[RaftServerEndpoint](_.runRaft())
      _    <- BitlapContext.fillNode(node)
      _    <- putStrLn(s"Raft Server started")
      _    <- ZIO.never
    } yield ())
      .onInterrupt(_ => putStrLn(s"Raft Server was interrupted").ignore)
}
final class RaftServerEndpoint(raftConfig: BitlapRaftConfig) {

  private lazy val LOG = LoggerFactory.getLogger(classOf[ElectionOnlyStateMachine])

  def runRaft(): Task[Node] = ZIO.effect {
    val dataPath       = raftConfig.dataPath
    val groupId        = raftConfig.groupId
    val serverIdStr    = raftConfig.serverAddress
    val initialConfStr = raftConfig.initialServerAddressList

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

    while (node.node == null)
      Thread.sleep(1000)
    node.node
  }
}

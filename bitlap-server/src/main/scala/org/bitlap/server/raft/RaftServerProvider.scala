/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.raft

import com.alipay.sofa.jraft.Node
import com.alipay.sofa.jraft.option.NodeOptions
import org.bitlap.network.ServerType
import org.bitlap.server.ServerProvider
import zio.console.putStrLn
import zio.{ Runtime => _, _ }
import org.bitlap.server.BitlapServerContext
import org.slf4j.LoggerFactory

/** bitlap raft cluster和rpc服务
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/10/28
 */
final class RaftServerProvider(raftServerConfig: RaftServerConfig) extends ServerProvider {

  private lazy val LOG = LoggerFactory.getLogger(classOf[ElectionOnlyStateMachine])

  private def runRaft(): Task[Node] = ZIO.effect {
    val dataPath       = raftServerConfig.dataPath
    val groupId        = raftServerConfig.groupId
    val serverIdStr    = raftServerConfig.serverAddress
    val initialConfStr = raftServerConfig.initialServerAddressList

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

  override def serverType: ServerType = ServerType.Raft

  override def service(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    ((runRaft().flatMap(fr => BitlapServerContext.fillNode(fr)) *> putStrLn(
      s"$serverType: Raft Server started"
    )) *> ZIO.never)
      .onInterrupt(putStrLn(s"$serverType: Raft Server stopped").ignore)
      .exitCode

}

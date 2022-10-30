/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.raft

import com.alipay.sofa.jraft.Node
import com.alipay.sofa.jraft.option.NodeOptions
import org.bitlap.network.ServerType
import org.bitlap.server.ServerProvider
import zio.console.putStrLn
import zio.{ ExitCode, Task, URIO, ZIO }

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/28
 */
final class RaftServerProvider(raftServerConfig: RaftServerConfig) extends ServerProvider with zio.App {

  // Start elections by 3 instance. Note that if multiple instances are started on the same machine,
  // the first parameter `dataPath` should not be the same.
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
        val serverId = node.getNode.getLeaderId
        val ip       = serverId.getIp
        val port     = serverId.getPort
        println("[ElectionBootstrap] Leader's ip is: " + ip + ", port: " + port)
        println("[ElectionBootstrap] Leader start on term: " + leaderTerm)
      }

      override def onLeaderStop(leaderTerm: Long): Unit =
        println("[ElectionBootstrap] Leader stop on term: " + leaderTerm)
    })

    Runtime.getRuntime.addShutdownHook(new Thread(() => node.shutdown()))

    node.init(electionOpts)

    node.getNode
  }

  override def serverType: ServerType = ServerType.Raft

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    (
      runRaft().flatMap(fr => ZIO.effect(RaftClient.init(fr))) *> putStrLn(s"$serverType: Raft Server started")
        .provideLayer(
          zio.console.Console.live
        )
    ).exitCode

  override def service(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    this.run(args)

}

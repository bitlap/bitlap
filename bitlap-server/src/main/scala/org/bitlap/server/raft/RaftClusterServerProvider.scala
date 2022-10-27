/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.raft
import com.ariskk.raft.model.{ NodeId, NodeState }
import com.ariskk.raft.statemachine.KeyValueStore
import org.bitlap.server.raft.storage.RocksDBStorage
import org.bitlap.server.ServerProvider
import zio._
import zio.console.{ putStrLn, Console }
import com.ariskk.raft.statemachine.{ Key, WriteKey }
import com.ariskk.raft.statemachine.ReadKey

import java.io.IOException

final class RaftClusterServerProvider(
  selfConfig: RaftServer.Config,
  configs: IndexedSeq[RaftServer.Config]
) extends ServerProvider {

  private def key(i: Int) = Key(s"key$i")

  private def intCommand(i: Int): WriteKey[Int] = WriteKey[Int](key(i), i)

  private val clientM = RaftClient.apply(configs)

  override def serverType: String = "RAFT_CLUSTER"

  def startCluster[T]() =
    for {
      storage          <- RocksDBStorage(s"/tmp/rocks-${selfConfig.nodeId.value}", "DB")
      stateMachine     <- KeyValueStore.apply[T]
      server           <- RaftServer(selfConfig, (configs.toSet - selfConfig).toSeq, storage, stateMachine)
      serverFiber      <- server.run.fork
      serverNoteId     <- server.raftRef.get.map(_.nodeId)
      serverNoteStates <- server.getState
    } yield (serverFiber, serverNoteId -> serverNoteStates)

  private def printNodeState(
    serverNode: (NodeId, NodeState)
  ): ZIO[Any, IOException, Unit] =
    for {
      _ <- putStrLn(s"======================================").provideLayer(Console.live)
      _ <- putStrLn(s"Current Server nodeState: $serverNode").provideLayer(Console.live)
      _ <- putStrLn(s"======================================").provideLayer(Console.live)
    } yield ()

  override def service(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    (for {
      (serverFiber, serverNoteStates) <- startCluster()
      _                               <- printNodeState(serverNoteStates)
      _ <- putStrLn(
        s"$serverType: Raft cluster server started successfully"
      )
      clients <- clientM
      w       <- clients.submitCommand(intCommand(1))
      r       <- clients.submitQuery[Int](ReadKey(key(1)))
      _ <- putStrLn(
        s"Test raft: write:$w, read: $r"
      )
      //      _ <- serverFiber.interrupt
//      _ <- clusterFiber.interrupt
    } yield ()).foldM(
      e => ZIO.fail(e).exitCode,
      _ => ZIO.effectTotal(ExitCode.success)
    )
}

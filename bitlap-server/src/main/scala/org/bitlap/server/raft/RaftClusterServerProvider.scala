/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.raft
import com.ariskk.raft.Raft
import com.ariskk.raft.model.{ NodeId, Serde }
import com.ariskk.raft.statemachine.KeyValueStore
import org.bitlap.server.raft.storage.RocksDBStorage
import org.bitlap.server.BitlapServerProvider
import scalapb.zio_grpc.{ ServerMain, ServiceList }
import zio._
import zio.console.{ putStrLn, Console }

private final class RaftClusterServerProvider[T](val raftPort: Int, raftRef: Ref[Raft[T]], serdeRef: Ref[Serde])
    extends ServerMain {

  override def port: Int = raftPort

  def services: ServiceList[zio.ZEnv] = ServiceList.addM(ZIO.succeed(ZioRaftService(raftRef, serdeRef)))

  override def run(args: List[String]): URIO[zio.ZEnv with Console, ExitCode] =
    (for {
      _ <- putStrLn(s"Raft server is listening to port: $raftPort")
      r <- super.run(args)
    } yield r).foldM(
      e => ZIO.fail(e).exitCode,
      _ => ZIO.effectTotal(ExitCode.success)
    )
}
object RaftClusterServerProvider extends BitlapServerProvider {

  override def serverType: String = "GRPC_RAFT_CLUSTERS"

  private def createRaftServer[T](
    config: RaftServer.Config,
    peers: Set[RaftServer.Config]
  ): ZIO[Any, Throwable, RaftServer[T]] =
    for {
      storage      <- RocksDBStorage(s"/tmp/rocks-${config.nodeId.value}", "DB")
      stateMachine <- KeyValueStore.apply[T]
      server       <- RaftServer(config, peers.toSeq, storage, stateMachine)
    } yield server

  private def generateConfigs(numberOfNodes: Int): IndexedSeq[RaftServer.Config] =
    (1 to numberOfNodes).map { i =>
      RaftServer.Config(
        NodeId.newUniqueId,
        "127.0.0.1",
        raftPort = 9700 + i,
        raftClientPort = 9800 + i
      )
    }

  def startServer(numberOfNodes: Int = 3): ZIO[Any, Throwable, IndexedSeq[(Int, Int)]] = {
    val configs = generateConfigs(numberOfNodes)
    for {
      servers <- ZIO.collectAll(configs.map(c => createRaftServer[Int](c, configs.toSet - c)))
      fibers  <- ZIO.collectAll(servers.map(_.run.fork))
      _       <- ZIO.collectAll(fibers.map(_.interrupt))
    } yield configs.map(c => c.raftPort -> c.raftClientPort)

  }

  override def service(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    (for {
      r <- startServer()
      _ <- putStrLn(
        s"$serverType: Raft cluster servers are listening to server port: ${r
            .map(_._1)
            .mkString(",")} and client port: ${r.map(_._2).mkString(",")}"
      )
    } yield r).foldM(
      e => ZIO.fail(e).exitCode,
      _ => ZIO.effectTotal(ExitCode.success)
    )
}

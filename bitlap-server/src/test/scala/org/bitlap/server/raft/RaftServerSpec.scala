/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.raft
import com.ariskk.raft.model.NodeId
import com.ariskk.raft.statemachine.{ Key, KeyValueStore, WriteKey }
import junit.framework.TestCase
import org.bitlap.server.raft.storage.RocksDBStorage
import zio._

/** raft server unit test
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,6/1/22
 */
class RaftServerSpec extends TestCase("RaftServerSpec") {

  private def createRaftServer[T](
    config: RaftServer.Config,
    peers: Set[RaftServer.Config]
  ): ZIO[Any, Throwable, RaftServer[T]] =
    for {
      storage      <- RocksDBStorage(s"/tmp/rocks-${config.nodeId.value}", "DB")
      stateMachine <- KeyValueStore.apply[T]
      server       <- RaftServer(config, peers.toSeq, storage, stateMachine)
    } yield server

  private def generateConfigs(numberOfNodes: Int = 3): IndexedSeq[RaftServer.Config] =
    (1 to numberOfNodes).map { i =>
      RaftServer.Config(
        NodeId.newUniqueId,
        "127.0.0.1",
        raftPort = 9700 + i,
        raftClientPort = 9800 + i
      )
    }

  def test_submitCommand(): Unit = {
    val configs = generateConfigs()
    val program = for {
      servers <- ZIO.collectAll(configs.map(c => createRaftServer[Int](c, configs.toSet - c)))
      client  <- RaftClient(configs)
      fibers  <- ZIO.collectAll(servers.map(_.run.fork))
      ret     <- ZIO.collectAll((1 to 5).map(i => client.submitCommand(WriteKey(Key(s"key-$i"), i))))
      _       <- ZIO.collectAll(fibers.map(_.interrupt))
    } yield ret.forall(r => r)

    val ret = zio.Runtime.default.unsafeRun(program)
    println(ret)
  }
}

/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.raft

import com.ariskk.raft.Raft
import com.ariskk.raft.model._
import com.ariskk.raft.statemachine.StateMachine
import com.ariskk.raft.storage._
import com.google.protobuf.ByteString
import io.grpc.ManagedChannelBuilder
import org.bitlap.network.raft.ZioRaft.RaftServiceClient
import org.bitlap.network.NetworkException.RpcException
import org.bitlap.network.raft.{ CommandType, RaftCommandReq }
import zio._
import io.grpc.Status

import scala.util.Try

/** raft server
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/6/1
 */
final class RaftServer[T](
  config: RaftServer.Config,
  peerConfig: Seq[RaftServer.Config],
  raftRef: Ref[Raft[T]],
  serdeRef: Ref[Serde]
) {

  private lazy val serverRaft: URIO[zio.ZEnv, ExitCode] =
    new RaftClusterServerProvider(config.raftPort, raftRef, serdeRef).run(Nil)

  private lazy val clientRaft: URIO[zio.ZEnv, ExitCode] =
    new RaftClusterServerProvider(config.raftClientPort, raftRef, serdeRef).run(Nil)

  private lazy val peerChannels: Map[NodeId, ZLayer[Any, Throwable, RaftServiceClient]] = peerConfig
    .map(config =>
      config.nodeId -> RaftServiceClient.live(
        scalapb.zio_grpc
          .ZManagedChannel(ManagedChannelBuilder.forAddress(config.address, config.raftPort).usePlaintext())
      )
    )
    .toMap

  private def sendMessage(m: Message): ZIO[Any, RpcException, Unit] = peerChannels
    .get(m.to)
    .fold[ZIO[Any, RpcException, Unit]](ZIO.unit) { channel =>
      for {
        serde <- serdeRef.get
        _ <- RaftServiceClient
          .submit(RaftCommandReq(ByteString.copyFrom(serde.serialize(m)), CommandType.MESSAGE))
          .provideLayer(channel)
          .mapError(e =>
            RpcException(msg = "raft sendMessage error", cause = Try(e.asInstanceOf[Status].asException()).toOption)
          )
      } yield ()
    }

  private lazy val sendMessages: ZIO[Any, RpcException, Unit] =
    for {
      raft <- raftRef.get
      _ <- raft.takeAll.flatMap { ms =>
        ZIO.collectAllPar(ms.map(sendMessage))
      }.forever
    } yield ()

  lazy val run: ZIO[Any, RpcException, Unit] =
    (for {
      raft <- raftRef.get
      _    <- raft.run.fork
      _    <- clientRaft.fork
      _    <- serverRaft.fork
      _    <- sendMessages
    } yield ()).provideLayer(ZEnv.live)

//  def getState: ZIO[Any, Nothing, NodeState] = raftRef.get.flatMap(_.nodeState)

}

object RaftServer {

  case class Config(
    nodeId: NodeId,
    address: String,
    raftPort: Int,
    raftClientPort: Int
  )

  def apply[T](
    config: Config,
    peerConfig: Seq[Config],
    storage: Storage,
    stateMachine: StateMachine[T]
  ): UIO[RaftServer[T]] = for {
    raft     <- Raft[T](config.nodeId, peerConfig.map(_.nodeId).toSet, storage, stateMachine)
    raftRef  <- Ref.make(raft)
    serdeRef <- Ref.make(Serde.kryo)
  } yield new RaftServer[T](config, peerConfig, raftRef, serdeRef)

}

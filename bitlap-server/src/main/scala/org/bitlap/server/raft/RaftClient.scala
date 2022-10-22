/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.raft

import com.ariskk.raft.model._
import com.ariskk.raft.model.Command.{ ReadCommand, WriteCommand }
import com.ariskk.raft.model.CommandResponse._
import com.google.protobuf.ByteString
import io.grpc.ManagedChannelBuilder
import org.bitlap.network.BitlapNetworkException.RpcException
import org.bitlap.network.raft.{ CommandType, RaftCommandReq }
import org.bitlap.network.raft.ZioRaft.RaftServiceClient
import zio._
import io.grpc.Status

import scala.util.Try
import scala.reflect.ClassTag
import scala.util.Random

/** raft client.
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,6/1/22
 */
final class RaftClient(nodes: Seq[RaftServer.Config], serdeRef: Ref[Serde], leaderRef: Ref[RaftServer.Config]) {

  private def shuffleLeaderRef: IO[Nothing, Unit] = leaderRef.set(nodes(Random.nextInt(nodes.size)))

  private def rpcClient(
    serde: Serde,
    command: Command,
    address: String,
    port: Int
  ): ZIO[Any, RpcException, Array[Byte]] = {
    val client: Layer[Throwable, RaftServiceClient] = RaftServiceClient.live(
      scalapb.zio_grpc.ZManagedChannel(ManagedChannelBuilder.forAddress(address, port).usePlaintext())
    )
    for {
      chunks <- RaftServiceClient
        .submit(RaftCommandReq(ByteString.copyFrom(serde.serialize(command)), CommandType.COMMAND))
        .provideLayer(client)
        .mapError(e => RpcException(msg = "rpc error", cause = Try(e.asInstanceOf[Status].asException()).toOption))
    } yield chunks.data.toByteArray
  }

  def submitCommand(command: WriteCommand): ZIO[Any, Throwable, Unit] =
    for {
      serde    <- serdeRef.get
      leader   <- leaderRef.get
      chunks   <- rpcClient(serde, command, leader.address, leader.raftClientPort)
      response <- ZIO.fromEither(serde.deserialize[CommandResponse](chunks))
      _ <- response match {
        case Committed =>
          ZIO.unit
        case LeaderNotFoundResponse =>
          shuffleLeaderRef *> submitCommand(command)
        case Redirect(leaderId) =>
          for {
            newLeader <- ZIO
              .fromOption(nodes.find(_.nodeId == leaderId))
              .mapError(_ => new Exception("Failed to find leader in node config"))
            _ <- leaderRef.set(newLeader)
            _ <- submitCommand(command)
          } yield ()
      }
    } yield ()

  def submitQuery[T: ClassTag](query: ReadCommand): ZIO[Any, Throwable, Option[T]] =
    for {
      serde    <- serdeRef.get
      leader   <- leaderRef.get
      chunks   <- rpcClient(serde, query, leader.address, leader.raftClientPort)
      response <- ZIO.effect(serde.deserialize[Option[T]](chunks).toOption.flatten)
    } yield response

}
object RaftClient {
  def apply(nodes: Seq[RaftServer.Config]): IO[RpcException, RaftClient] =
    for {
      serdeRef  <- Ref.make(Serde.kryo)
      leader    <- ZIO.fromOption(nodes.headOption).mapError(_ => RpcException(msg = "raft error"))
      leaderRef <- Ref.make(leader)
    } yield new RaftClient(nodes, serdeRef, leaderRef)
}

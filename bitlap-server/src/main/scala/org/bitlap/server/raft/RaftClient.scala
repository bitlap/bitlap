/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.raft

import com.ariskk.raft.model._
import com.ariskk.raft.model.Command.{ ReadCommand, WriteCommand }
import com.ariskk.raft.model.CommandResponse._
import org.bitlap.network.NetworkException.RpcException
import zio._
import zio.duration.Duration

import java.util.concurrent.TimeUnit
import scala.reflect.ClassTag
import scala.util.Random
import com.google.protobuf.ByteString

/** The raft client only for Command request, not vote.
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,6/1/22
 */
final class RaftClient(nodes: Seq[RaftServer.Config], serdeRef: Ref[Serde], leaderRef: Ref[RaftServer.Config])
    extends Invoker {

  private def shuffleLeaderRef: IO[Nothing, Unit] = leaderRef.set(nodes(Random.nextInt(nodes.size)))

  def submitCommand(command: WriteCommand): ZIO[Any, Throwable, Boolean] =
    for {
      serde  <- serdeRef.get
      leader <- leaderRef.get
      chunks <- rpcClient(
        ByteString.copyFrom(serde.serialize(command)),
        leader.address,
        leader.raftClientPort
      )
      response <- ZIO.fromEither(serde.deserialize[CommandResponse](chunks))
      res <- response match {
        case Committed =>
          ZIO.succeed(true)
        case LeaderNotFoundResponse =>
          shuffleLeaderRef *> submitCommand(command)
        case Redirect(leaderId) =>
          for {
            newLeader <- ZIO
              .fromOption(nodes.find(_.nodeId == leaderId))
              .mapError(_ => new Exception("Failed to find leader in node config"))
            _ <- leaderRef.set(newLeader)
            r <- submitCommand(command)
          } yield r
      }
    } yield res

  def submitQuery[T: ClassTag](query: ReadCommand): ZIO[Any, Throwable, Option[T]] =
    for {
      serde    <- serdeRef.get
      leader   <- leaderRef.get
      chunks   <- rpcClient(ByteString.copyFrom(serde.serialize(query)), leader.address, leader.raftClientPort)
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

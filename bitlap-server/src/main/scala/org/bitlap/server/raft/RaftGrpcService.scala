/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.raft

import com.ariskk.raft.Raft
import com.ariskk.raft.model.{ Command, Message, RaftException, Serde }
import com.ariskk.raft.model.Command.WriteCommand
import com.google.protobuf.ByteString
import io.grpc.Status
import org.bitlap.network.RpcStatus
import org.bitlap.network.raft.{ CommandType, RaftCommandReq, RaftCommandResp }
import org.bitlap.network.raft.ZioRaft.ZRaftService
import org.bitlap.tools.apply
import zio._
import zio.clock.Clock
import com.ariskk.raft.model.Command.ReadCommand

/** raft service
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/6/1
 */
@apply
final class RaftGrpcService[T](raftRef: Ref[Raft[T]], serdeRef: Ref[Serde])
    extends ZRaftService[Any, Any]
    with RpcStatus {

  override def submit(request: RaftCommandReq): ZIO[Any, Status, RaftCommandResp] = {
    val data = request.data.toByteArray
    (request.`type` match {
      case CommandType.COMMAND => processCommand(data)
      case CommandType.MESSAGE => processMessage(data)
    })
      .mapError(_ => Status.UNAVAILABLE)
      .map(r => RaftCommandResp(ByteString.copyFrom(r)))
      .provideLayer(Clock.live)
  }

  private def processMessage(bytes: Array[Byte]): ZIO[Clock, RaftException, Array[Byte]] = for {
    serde   <- serdeRef.get
    message <- ZIO.fromEither(serde.deserialize[Message](bytes))
    raft    <- raftRef.get
    _       <- raft.offerMessage(message)
  } yield Array.empty

  private def processCommand(bytes: Array[Byte]): ZIO[Clock, RaftException, Array[Byte]] =
    for {
      serde   <- serdeRef.get
      command <- ZIO.fromEither(serde.deserialize[Command](bytes))
      raft    <- raftRef.get
      response <- command match {
        case w: WriteCommand => raft.submitCommand(w)
        case r: ReadCommand  => raft.submitQuery(r)
      }
    } yield serde.serialize(response)
}

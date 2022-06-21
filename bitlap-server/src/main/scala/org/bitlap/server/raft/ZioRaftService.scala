/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.raft

import com.ariskk.raft.Raft
import com.ariskk.raft.model.{ Command, Message, RaftException, Serde }
import com.ariskk.raft.model.Command.{ ReadCommand, WriteCommand }
import com.google.protobuf.ByteString
import io.grpc.Status
import org.bitlap.network.RpcStatusBuilder
import org.bitlap.network.raft.{ CommandType, RaftCommandReq, RaftCommandResp }
import org.bitlap.network.raft.ZioRaft.ZRaftService
import org.bitlap.tools.apply
import scalapb.zio_grpc.{ ServerMain, ServiceList }
import zio.{ ExitCode, Ref, URIO, ZIO }
import zio.clock.Clock
import zio.console.{ putStrLn, Console }

/** raft service
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/6/1
 */
@apply
final class ZioRaftService[T](raftRef: Ref[Raft[T]], serdeRef: Ref[Serde])
    extends ZRaftService[Any, Any]
    with RpcStatusBuilder {

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
//        case r: ReadCommand  => raft.submitQuery(r)
      }
    } yield serde.serialize(response)
}

object ZioRaftService {

  @apply
  final class Server[T](val raftPort: Int, raftRef: Ref[Raft[T]], serdeRef: Ref[Serde]) extends ServerMain {

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
}

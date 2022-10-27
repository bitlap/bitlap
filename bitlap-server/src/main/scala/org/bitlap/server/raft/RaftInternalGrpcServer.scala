/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.raft

import com.ariskk.raft.Raft
import com.ariskk.raft.model.Serde
import scalapb.zio_grpc.{ ServerMain, ServiceList }
import zio._
import zio.console.{ putStrLn, Console }

final class RaftInternalGrpcServer[T](val raftPort: Int, raftRef: Ref[Raft[T]], serdeRef: Ref[Serde])
    extends ServerMain {

  override def port: Int = raftPort

  /** run raft grpc server
   */
  def services: ServiceList[zio.ZEnv] = ServiceList.addM(ZIO.succeed(RaftGrpcService(raftRef, serdeRef)))

  override def run(args: List[String]): URIO[zio.ZEnv with Console, ExitCode] =
    (for {
      _ <- putStrLn(s"Raft server is listening to port: $raftPort")
      r <- super.run(args)
    } yield r).foldM(
      e => ZIO.fail(e).exitCode,
      _ => ZIO.effectTotal(ExitCode.success)
    )
}

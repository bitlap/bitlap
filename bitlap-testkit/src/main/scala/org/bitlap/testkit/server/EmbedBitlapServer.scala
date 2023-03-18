/* Copyright (c) 2023 bitlap.org */
package org.bitlap.testkit.server

import org.bitlap.server.{ GrpcServerEndpoint, RaftServerEndpoint }
import org.bitlap.server.config._
import zio.console.putStrLn
import zio._
import zio.magic._

/** bitlap 嵌入式服务 包含http,grpc,raft
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/27
 */

object EmbedBitlapServer extends zio.App {

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    (for {
      _ <- RaftServerEndpoint.service(args).fork
      _ <- GrpcServerEndpoint.service(args).fork
      _ <- putStrLn("""
                        |    __    _ __  __
                        |   / /_  (_) /_/ /___ _____
                        |  / __ \/ / __/ / __ `/ __ \
                        | / /_/ / / /_/ / /_/ / /_/ /
                        |/_.___/_/\__/_/\__,_/ .___/
                        |                   /_/
                        |""".stripMargin)
    } yield ())
      .inject(
        RaftServerEndpoint.live,
        GrpcServerEndpoint.live,
        zio.ZEnv.live,
        BitlapGrpcConfig.live,
        BitlapRaftConfig.live
      )
      .foldM(
        e => ZIO.fail(e).exitCode,
        _ => ZIO.effectTotal(ExitCode.success)
      )

}

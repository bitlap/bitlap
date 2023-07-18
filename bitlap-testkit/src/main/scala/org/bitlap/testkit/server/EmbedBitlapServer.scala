/* Copyright (c) 2023 bitlap.org */
package org.bitlap.testkit.server

import org.bitlap.server.*
import org.bitlap.server.config.*
import org.bitlap.server.service.DriverGrpcService

import zio.*
import zio.ZIOAppArgs.getArgs

/** bitlap 嵌入式服务 包含http,grpc,raft
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/27
 */

object EmbedBitlapServer extends zio.ZIOAppDefault {

  override def run =
    (for {
      args <- getArgs
      t1   <- RaftServerEndpoint.service(args.toList).fork
      t2   <- GrpcServerEndpoint.service(args.toList).fork
      _ <- Console.printLine("""
                        |    __    _ __  __
                        |   / /_  (_) /_/ /___ _____
                        |  / __ \/ / __/ / __ `/ __ \
                        | / /_/ / / /_/ / /_/ / /_/ /
                        |/_.___/_/\__/_/\__,_/ .___/
                        |                   /_/
                        |""".stripMargin)
      _ <- ZIO.collectAll(Seq(t1.join, t2.join))
    } yield ())
      .provide(
        RaftServerEndpoint.live,
        GrpcServerEndpoint.live,
        Scope.default,
        MockDriverIO.live,
        ZIOAppArgs.empty,
        DriverGrpcService.live,
        BitlapServerConfiguration.testLive
      )
      .fold(
        e => ZIO.fail(e).exitCode,
        _ => ZIO.succeed(ExitCode.success)
      )
}

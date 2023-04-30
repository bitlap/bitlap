/* Copyright (c) 2023 bitlap.org */
package org.bitlap.testkit.server

import org.bitlap.server.*
import org.bitlap.server.config.*
import org.bitlap.server.rpc.GrpcServiceLive
import zio.ZIOAppArgs.getArgs
import zio.*

/** bitlap 嵌入式服务 包含http,grpc,raft
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/27
 */

object EmbedBitlapServer extends zio.ZIOAppDefault {

  override def run =
    (for {
      args <- getArgs
      _    <- RaftServerEndpoint.service(args.toList).fork
      _    <- GrpcServerEndpoint.service(args.toList).fork
      _ <- Console.printLine("""
                        |    __    _ __  __
                        |   / /_  (_) /_/ /___ _____
                        |  / __ \/ / __/ / __ `/ __ \
                        | / /_/ / / /_/ / /_/ / /_/ /
                        |/_.___/_/\__/_/\__,_/ .___/
                        |                   /_/
                        |""".stripMargin)
    } yield ())
      .provide(
        RaftServerEndpoint.live,
        GrpcServerEndpoint.live,
        BitlapGrpcConfig.live,
        BitlapRaftConfig.live,
        Scope.default,
        MockAsyncRpcBackend.live,
        ZIOAppArgs.empty,
        GrpcServiceLive.live
      )
      .fold(
        e => ZIO.fail(e).exitCode,
        _ => ZIO.succeed(ExitCode.success)
      )
}

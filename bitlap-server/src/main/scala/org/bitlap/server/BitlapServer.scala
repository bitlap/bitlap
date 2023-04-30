/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server

import org.bitlap.server.config.*
import org.bitlap.server.http.HttpServiceLive
import org.bitlap.server.rpc.{ GrpcBackendLive, GrpcServiceLive }
import org.bitlap.server.session.SessionManager
import zio.*

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import zio.Duration as ZDuration

/** bitlap 聚合服务
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/10/19
 */
object BitlapServer extends zio.ZIOAppDefault {

  // 在java 9以上运行时，需要JVM参数: --add-exports java.base/jdk.internal.ref=ALL-UNNAMED
  override def run =
    (for {
      args <- getArgs
      t1   <- RaftServerEndpoint.service(args.toList).fork
      t2   <- GrpcServerEndpoint.service(args.toList).fork
      t3   <- HttpServerEndpoint.service(args.toList).fork
      _ <- SessionManager
        .startListener()
        .repeat(Schedule.fixed(ZDuration.fromScala(Duration(3000, TimeUnit.MILLISECONDS))))
        .forkDaemon
      _ <- Console.printLine("""
                      |    __    _ __  __
                      |   / /_  (_) /_/ /___ _____
                      |  / __ \/ / __/ / __ `/ __ \
                      | / /_/ / / /_/ / /_/ / /_/ /
                      |/_.___/_/\__/_/\__,_/ .___/
                      |                   /_/
                      |""".stripMargin)
      _ <- ZIO.collectAll(Seq(t1.join, t2.join, t3.join))
    } yield ())
      .provide(
        RaftServerEndpoint.live,
        GrpcServerEndpoint.live,
        HttpServerEndpoint.live,
        BitlapGrpcConfig.live,
        BitlapHttpConfig.live,
        BitlapRaftConfig.live,
        HttpServiceLive.live,
        SessionManager.live,
        GrpcBackendLive.live,
        Scope.default,
        ZIOAppArgs.empty,
        GrpcServiceLive.live
      )
      .fold(
        e => ZIO.fail(e).exitCode,
        _ => ZIO.attempt(ExitCode.success)
      )
      .onTermination(_ => Console.printLine(s"Bitlap Server shutdown now").ignore)
      .onExit(_ => Console.printLine(s"Bitlap Server stopped").ignore)
      .onInterrupt(_ => Console.printLine(s"Bitlap Server was interrupted").ignore)
}

/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.*

import org.bitlap.server.config.*
import org.bitlap.server.http.HttpServiceLive
import org.bitlap.server.service.*
import org.bitlap.server.session.SessionManager

import zio.{ Duration as ZDuration, * }
import zio.logging.LogFormat
import zio.logging.backend.SLF4J

/** bitlap 聚合服务
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/10/19
 */
object BitlapServer extends zio.ZIOAppDefault:

  private lazy val logger = Runtime.removeDefaultLoggers >>> SLF4J.slf4j(LogFormat.colored)

  // 在java 9以上运行时，需要JVM参数: --add-exports java.base/jdk.internal.ref=ALL-UNNAMED
  override def run =
    (for {
      args         <- getArgs
      t1           <- RaftServerEndpoint.service(args.toList).fork
      t2           <- GrpcServerEndpoint.service(args.toList).fork
      t3           <- HttpServerEndpoint.service(args.toList).fork
      serverConfig <- ZIO.serviceWith[BitlapServerConfiguration](_.sessionConfig)
      _ <- SessionManager
        .startListener()
        .repeat(Schedule.fixed(ZDuration.fromScala(serverConfig.interval)))
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
        HttpServiceLive.live,
        SessionManager.live,
        DriverServiceLive.live,
        Scope.default,
        ZIOAppArgs.empty,
        DriverGrpcService.live,
        BitlapServerConfiguration.live,
        logger
      )
      .fold(
        e => ZIO.fail(e).exitCode,
        _ => ZIO.attempt(ExitCode.success)
      )
      .onTermination(_ => Console.printLine(s"Bitlap Server shutdown now").ignore)
      .onExit(_ => Console.printLine(s"Bitlap Server stopped").ignore)
      .onInterrupt(_ => Console.printLine(s"Bitlap Server was interrupted").ignore)

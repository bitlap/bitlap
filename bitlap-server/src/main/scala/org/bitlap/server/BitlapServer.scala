/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.server

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.*

import org.bitlap.client.{ AsyncClient, ClientConfig }
import org.bitlap.server.config.*
import org.bitlap.server.http.HttpServiceLive
import org.bitlap.server.service.*
import org.bitlap.server.session.SessionManager

import zio.{ Duration as ZDuration, * }
import zio.logging.LogFormat
import zio.logging.backend.SLF4J

/** Bitlap aggregation Services
 */
object BitlapServer extends ZIOAppDefault:

  private lazy val logger = Runtime.removeDefaultLoggers >>> SLF4J.slf4j(LogFormat.colored)

  // When running Java 9 or above, JVM parameters are required: --add-exports java.base/jdk.internal.ref=ALL-UNNAMED
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
      _ <- ZIO.serviceWithZIO[ServerNodeContext](_.start())
      _ <- ZIO.collectAll(Seq(t1.join, t2.join, t3.join))
    } yield ())
      .provide(
        RaftServerEndpoint.live,
        GrpcServerEndpoint.live,
        HttpServerEndpoint.live,
        HttpServiceLive.live,
        SessionManager.live,
        DriverService.live,
        Scope.default,
        ZIOAppArgs.empty,
        DriverGrpcService.live,
        BitlapServerConfiguration.live,
        ServerNodeContext.live,
        logger
      )
      .fold(
        e => ZIO.fail(e).exitCode,
        _ => ZIO.attempt(ExitCode.success)
      )
      .onTermination(_ => Console.printLine(s"Bitlap Server shutdown now").ignore)
      .onExit(_ => Console.printLine(s"Bitlap Server stopped").ignore)
      .onInterrupt(_ => Console.printLine(s"Bitlap Server was interrupted").ignore)

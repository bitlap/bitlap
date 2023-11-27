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

import scala.concurrent.duration.*

import org.bitlap.server.config.*
import org.bitlap.server.http.HttpServiceLive
import org.bitlap.server.service.*
import org.bitlap.server.session.SessionManager

import zio.{ Duration as ZDuration, * }

/** Bitlap aggregation Services
 */
object BitlapServer extends ZIOAppDefault:

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] = BitlapLogFormat.slf4j

  override def run =
    (for {
      args           <- getArgs
      raft           <- RaftServerEndpoint.service(args.toList).fork
      grpc           <- GrpcServerEndpoint.service(args.toList).fork
      http           <- HttpServerEndpoint.service(args.toList).fork
      serverConfig   <- ZIO.serviceWith[BitlapConfiguration](_.sessionConfig)
      sessionManager <- ZIO.service[SessionManager]
      _ <- sessionManager
        .startListener()
        .repeat(Schedule.fixed(ZDuration.fromScala(serverConfig.interval)))
        .forkDaemon
      _ <- ZIO.logInfo("""
                      |    __    _ __  __
                      |   / /_  (_) /_/ /___ _____
                      |  / __ \/ / __/ / __ `/ __ \
                      | / /_/ / / /_/ / /_/ / /_/ /
                      |/_.___/_/\__/_/\__,_/ .___/
                      |                   /_/
                      |""".stripMargin)
      _ <- ZIO.serviceWithZIO[BitlapGlobalContext](_.startFinished())
      _ <- ZIO.serviceWithZIO[BitlapGlobalContext](_.setSessionManager(sessionManager))
      _ <- ZIO.collectAll(Seq(raft.join, grpc.join, http.join))
    } yield ())
      .provide(
        RaftServerEndpoint.live,
        GrpcServerEndpoint.live,
        HttpServerEndpoint.live,
        HttpServiceLive.live,
        SessionManager.live,
        AsyncServerService.live,
        Scope.default,
        ZIOAppArgs.empty,
        DriverGrpcServer.live,
        BitlapConfiguration.live,
        BitlapGlobalContext.live
      )
      .fold(
        e => ZIO.fail(e).exitCode,
        _ => ZIO.attempt(ExitCode.success)
      )
      .onTermination(_ => ZIO.logError(s"Bitlap Server shutdown now"))
      .onExit(_ => ZIO.logInfo(s"Bitlap Server stopped"))
      .onInterrupt(_ => ZIO.logWarning(s"Bitlap Server was interrupted"))

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
package org.bitlap.testkit

import org.bitlap.server.*
import org.bitlap.server.config.*
import org.bitlap.server.http.HttpRoutes
import org.bitlap.server.http.route.*
import org.bitlap.server.http.service.*
import org.bitlap.server.service.*
import org.bitlap.server.session.SessionManager

import zio.*

/** Bitlap embedded services include GRPC and Raft
 */
object MockBitlapServer extends ZIOAppDefault {

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] = BitlapLogFormat.slf4j

  override def run =
    (for {
      args           <- getArgs
      t1             <- RaftServerEndpoint.service(args.toList).fork
      t2             <- GrpcServerEndpoint.service(args.toList).fork
      t3             <- HttpServerEndpoint.service(args.toList).fork
      sessionManager <- ZIO.service[SessionManager]
      // add http server?
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
      _ <- ZIO.collectAll(Seq(t1.join, t2.join, t3.join))
    } yield ())
      .provide(
        RaftServerEndpoint.live,
        GrpcServerEndpoint.live,
        HttpServerEndpoint.live,
        Scope.default,
        MockAsync.live,
        ZIOAppArgs.empty,
        DriverGrpcServer.live,
        BitlapConfigWrapper.testLive,
        BitlapGlobalContext.live,
        SessionManager.live,
        // http
        HttpRoutes.live,
        ResourceRoute.live,
        SqlRoute.live,
        SqlService.live,
        UserRoute.live,
        UserService.live,
        AccountAuthenticator.live
      )
      .fold(
        e => ZIO.fail(e).exitCode,
        _ => ZIO.succeed(ExitCode.success)
      )
}

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

import org.bitlap.network.ClientConfig
import org.bitlap.network.Driver.ZioDriver.{ DriverService as _, ZDriverService }
import org.bitlap.network.protocol.AsyncProtocol
import org.bitlap.network.protocol.impl.Async
import org.bitlap.server.config.BitlapConfiguration
import org.bitlap.server.service.*
import org.bitlap.server.session.SessionManager

import io.grpc.ServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import scalapb.zio_grpc
import scalapb.zio_grpc.*
import zio.*

/** Bitlap GRPC service
 */
object GrpcServerEndpoint:

  lazy val live: ZLayer[BitlapConfiguration, Nothing, GrpcServerEndpoint] =
    ZLayer.fromFunction((config: BitlapConfiguration) => new GrpcServerEndpoint(config))

  def service(
    args: List[String]
  ): ZIO[
    DriverGrpcService & Scope & GrpcServerEndpoint & BitlapNodeContext & BitlapConfiguration & SessionManager,
    Throwable,
    Unit
  ] =
    (for {
      config <- ZIO.service[BitlapConfiguration]
      _      <- ZIO.serviceWithZIO[GrpcServerEndpoint](_.runGrpcServer())
      client <- Async
        .make(
          ClientConfig(Map.empty, config.grpcConfig.getSplitPeers)
        )
        .build
      _ <- ZIO.logInfo(s"Grpc Server started at port: ${config.grpcConfig.port}")
      _ <- ZIO.serviceWithZIO[BitlapNodeContext](_.setProtocolImpl(client.get))
      _ <- ZIO.never
    } yield ())
      .onInterrupt(_ => ZIO.logWarning(s"Grpc Server was interrupted! Bye!"))

end GrpcServerEndpoint

final class GrpcServerEndpoint(config: BitlapConfiguration):

  private val serverLayer =
    ServerLayer.fromServiceList(
      io.grpc.ServerBuilder.forPort(config.grpcConfig.port).addService(ProtoReflectionService.newInstance()),
      ServiceList
        .addFromEnvironment[ZDriverService[RequestContext]]
    )

  private def runGrpcServer(): URIO[BitlapNodeContext & SessionManager, ExitCode] = ZLayer
    .makeSome[BitlapNodeContext & SessionManager, Server](
      serverLayer,
      DriverGrpcService.live,
      DriverService.live
    )
    .launch
    .exitCode

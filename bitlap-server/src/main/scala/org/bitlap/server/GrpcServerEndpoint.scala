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

import org.bitlap.client.AsyncClient
import org.bitlap.client.ClientConfig
import org.bitlap.network.AsyncProtocol
import org.bitlap.network.Driver.ZioDriver.{ DriverService as _, ZDriverService }
import org.bitlap.server.config.BitlapServerConfiguration
import org.bitlap.server.service.*

import io.grpc.ServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import scalapb.zio_grpc
import scalapb.zio_grpc.*
import zio.*

/** Bitlap GRPC service
 */
object GrpcServerEndpoint:

  lazy val live: ZLayer[BitlapServerConfiguration, Nothing, GrpcServerEndpoint] =
    ZLayer.fromFunction((config: BitlapServerConfiguration) => new GrpcServerEndpoint(config))

  def service(
    args: List[String]
  ): ZIO[
    DriverGrpcService with Scope with GrpcServerEndpoint with ServerNodeContext with BitlapServerConfiguration,
    Throwable,
    Unit
  ] =
    (for {
      _      <- Console.printLine(s"Grpc Server started")
      config <- ZIO.service[BitlapServerConfiguration]
      _      <- ZIO.serviceWithZIO[GrpcServerEndpoint](_.runGrpcServer())
      client <- AsyncClient
        .make(
          ClientConfig(Map.empty, config.grpcConfig.getSplitPeers)
        )
        .build
      _ <- ZIO.serviceWithZIO[ServerNodeContext](_.setClient(client.get))
      _ <- ZIO.never
    } yield ())
      .onInterrupt(_ => Console.printLine(s"Grpc Server was interrupted").ignore)

end GrpcServerEndpoint

final class GrpcServerEndpoint(val config: BitlapServerConfiguration):

  private val serverLayer =
    ServerLayer.fromServiceList(
      io.grpc.ServerBuilder.forPort(config.grpcConfig.port).addService(ProtoReflectionService.newInstance()),
      ServiceList
        .addFromEnvironment[ZDriverService[RequestContext]]
    )

  private def runGrpcServer(): URIO[ServerNodeContext, ExitCode] = ZLayer
    .makeSome[ServerNodeContext, Server](
      serverLayer,
      DriverGrpcService.live,
      DriverService.live
    )
    .launch
    .exitCode

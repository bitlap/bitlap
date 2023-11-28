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

import org.bitlap.server.config.BitlapConfiguration
import org.bitlap.server.http.HttpRoutes

import zio.{ ExitCode, ZIO, ZLayer }
import zio.http.Server
import zio.http.netty.NettyConfig
import zio.http.netty.NettyConfig.LeakDetectionLevel

/** Bitlap HTTP service
 */
object HttpServerEndpoint:

  val live: ZLayer[BitlapConfiguration & HttpRoutes, Nothing, HttpServerEndpoint] =
    ZLayer.fromFunction((config: BitlapConfiguration, httpRoutes: HttpRoutes) =>
      new HttpServerEndpoint(config, httpRoutes)
    )

  def service(args: List[String]): ZIO[HttpServerEndpoint, Nothing, ExitCode] =
    ZIO.serviceWithZIO[HttpServerEndpoint](_.runHttpServer())

end HttpServerEndpoint

final class HttpServerEndpoint(config: BitlapConfiguration, httpRoutes: HttpRoutes) {

  private def runHttpServer(): ZIO[Any, Nothing, ExitCode] =
    (Server
      .install(httpRoutes.getHttpApp)
      .flatMap(port => ZIO.logInfo(s"HTTP Server started at port: $port")) *> ZIO.never)
      .provide(
        ZLayer.succeed(
          Server.Config.default
            .port(config.httpConfig.port)
        ),
        ZLayer.succeed(
          NettyConfig.default
            .leakDetection(LeakDetectionLevel.PARANOID)
            .maxThreads(config.httpConfig.threads)
        ),
        Server.customized
      )
      .exitCode
      .onInterrupt(_ => ZIO.logWarning(s"HTTP Server was interrupted! Bye!"))
}

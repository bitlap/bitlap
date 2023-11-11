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

import java.io.IOException
import java.sql.DriverManager
import java.util.Properties

import org.bitlap.common.exception.BitlapException
import org.bitlap.server.config.BitlapConfiguration
import org.bitlap.server.config.BitlapHttpConfig
import org.bitlap.server.http.*

import io.circe.generic.auto.*
import io.circe.syntax.EncoderOps
import sttp.tapir.server.ziohttp.ZioHttpInterpreter
import sttp.tapir.swagger.bundle.SwaggerInterpreter
import sttp.tapir.ztapir.*
import zio.*
import zio.http.*
import zio.http.codec.*
import zio.http.netty.NettyConfig
import zio.http.netty.NettyConfig.LeakDetectionLevel

/** Bitlap HTTP service
 */
object HttpServerEndpoint:

  lazy val live: ZLayer[BitlapConfiguration & HttpServiceLive, Nothing, HttpServerEndpoint] =
    ZLayer.fromFunction((config: BitlapConfiguration, httpServiceLive: HttpServiceLive) =>
      new HttpServerEndpoint(config, httpServiceLive)
    )

  def service(args: List[String]): ZIO[HttpServerEndpoint, Nothing, ExitCode] =
    ZIO.serviceWithZIO[HttpServerEndpoint](_.runHttpServer())

end HttpServerEndpoint

final class HttpServerEndpoint(config: BitlapConfiguration, httpServiceLive: HttpServiceLive) extends HttpEndpoint:

  Class.forName(classOf[org.bitlap.Driver].getCanonicalName)

  private lazy val runServerEndpoint: ZServerEndpoint[Any, Any] = runEndpoint.zServerLogic { sql =>
    val sqlInput = sql.asJson.as[SqlInput].getOrElse(SqlInput(""))
    ZIO
      .attempt(httpServiceLive.execute(sqlInput.sql))
      .mapError(f => BitlapException("Unknown Error", cause = Option(f)))
  }

  private lazy val statusServerEndpoint: ZServerEndpoint[Any, Any] =
    statusEndpoint.zServerLogic { _ =>
      ZIO.succeed("""{"status":"ok"}""")
    }

  private val swaggerEndpoints: List[ZServerEndpoint[Any, Any]] =
    SwaggerInterpreter()
      .fromEndpoints[Task](List(runEndpoint, statusEndpoint), "Bitlap API", "1.0")

  private lazy val routes: http.HttpApp[Any, Throwable] =
    ZioHttpInterpreter().toHttp(List(runServerEndpoint, statusServerEndpoint) ++ swaggerEndpoints)

  private val indexHtml: http.HttpApp[Any, Throwable] = Http.fromResource(s"static/index.html")

  private val staticApp: http.HttpApp[Any, Throwable] = Http.collectHttp[Request] {
    case Method.GET -> !! / "init" =>
      // When using initialization, enable this
      val properties = new Properties()
      properties.put("bitlapconf:retries", "1")
      properties.put("bitlapconf:initFile", "conf/initFileForTest.sql")
      DriverManager.getConnection("jdbc:bitlap://localhost:23333/default", properties)
      indexHtml
    case req
        if req.method == Method.GET
          && req.path.startsWith(!! / "pages") =>
      indexHtml
    case Method.GET -> !! / path => Http.fromResource(s"static/$path")
    case _ =>
      indexHtml
  }

  private def runHttpServer(): ZIO[Any, Nothing, ExitCode] =
    (Server
      .install(routes.withDefaultErrorResponse ++ staticApp.withDefaultErrorResponse)
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

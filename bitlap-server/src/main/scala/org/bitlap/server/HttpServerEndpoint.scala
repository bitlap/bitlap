/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server

import io.circe.generic.auto.*
import io.circe.syntax.EncoderOps
import org.bitlap.network.NetworkException.SQLExecutedException
import org.bitlap.server.config.BitlapHttpConfig
import org.bitlap.server.http.*
import sttp.tapir.server.ziohttp.ZioHttpInterpreter
import sttp.tapir.swagger.bundle.SwaggerInterpreter
import sttp.tapir.ztapir.*
import zio.*
import zio.http.*
import zio.http.model.*

import java.io.IOException
import java.sql.DriverManager
import java.util.Properties

/** bitlap http服务
 *
 *  初始化数据接口: http://localhost:8080/init
 *
 *  查询数据接口: http://localhost:8080/sql
 *  @param port
 */
object HttpServerEndpoint {
  lazy val live: ZLayer[BitlapHttpConfig with HttpServiceLive, Nothing, HttpServerEndpoint] =
    ZLayer.fromFunction((config: BitlapHttpConfig, httpServiceLive: HttpServiceLive) =>
      new HttpServerEndpoint(config, httpServiceLive)
    )

  def service(args: List[String]): ZIO[HttpServerEndpoint, IOException, Unit] =
    (for {
      _ <- ZIO.serviceWithZIO[HttpServerEndpoint](_.httpServer())
      _ <- Console.printLine(s"HTTP Server started")
      _ <- ZIO.never
    } yield ())
      .onInterrupt(_ => Console.printLine(s"HTTP Server was interrupted").ignore)
}
final class HttpServerEndpoint(config: BitlapHttpConfig, httpServiceLive: HttpServiceLive) extends HttpEndpoint {

  private lazy val runServerEndpoint: ZServerEndpoint[Any, Any] = runEndpoint.zServerLogic { sql =>
    val sqlInput = sql.asJson.as[SqlInput].getOrElse(SqlInput(""))
    ZIO
      .attempt(httpServiceLive.execute(sqlInput.sql))
      .mapError(f => SQLExecutedException(msg = "Unknown Error", cause = Option(f)))
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
  private val staticApp: http.HttpApp[Any, Throwable] = Http.collectRoute[Request] {
    case Method.GET -> !! / "init" =>
      // 使用初始化时，开启这个
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

  def httpServer(): URIO[Any, ExitCode] =
    Server
      .serve(routes.withDefaultErrorResponse ++ staticApp.withDefaultErrorResponse)
      .provide(
        ServerConfig.live(ServerConfig.default.port(config.port)),
        Server.live
      )
      .exitCode

}

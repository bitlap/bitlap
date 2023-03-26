/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server

import org.bitlap.server.config.BitlapHttpConfig
import org.bitlap.server.http._
import zhttp.http._
import zhttp.service._
import zio._
import zio.blocking.Blocking
import zio.console._

import java.sql.DriverManager
import java.util.Properties

/** bitlap http服务
 *
 *  初始化数据接口： http://localhost:8080/init
 *
 *  查询数据接口：http://localhost:8080/sql
 *  @param port
 */
object HttpServerEndpoint {
  lazy val live: ZLayer[Has[BitlapHttpConfig] with Has[HttpServiceLive], Nothing, Has[HttpServerEndpoint]] =
    ZLayer.fromServices[BitlapHttpConfig, HttpServiceLive, HttpServerEndpoint]((config, httpServiceLive) =>
      new HttpServerEndpoint(config, httpServiceLive)
    )

  def service(args: List[String]): ZIO[
    Console with Blocking with EventLoopGroup with ServerChannelFactory with Has[HttpServerEndpoint],
    Throwable,
    Unit
  ] =
    (for {
      server <- ZIO
        .serviceWith[HttpServerEndpoint](_.httpServer)
      _ <- server.make.use(_ => putStrLn(s"HTTP Server started"))
      _ <- ZIO.never
    } yield ())
      .onInterrupt(_ => putStrLn(s"HTTP Server was interrupted").ignore)
}
final class HttpServerEndpoint(config: BitlapHttpConfig, httpServiceLive: HttpServiceLive) {

  private val app = Http.collectZIO[Request] {
    case req @ Method.POST -> !! / "api" / "sql" / "run" =>
      req.data.toJson.map { body =>
        val sql = body.hcursor.get[String]("sql").getOrElse("")
        httpServiceLive.execute(sql)
      }
    case Method.GET -> !! / "api" / "common" / "status" => ZIO.effect(Response.json(s"""{"status":"ok"}"""))
  }

  private val indexHtml = Http.fromResource(s"static/index.html")
  private val staticApp = Http.collectHttp[Request] {
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

  def httpServer: UIO[Server[Blocking, Throwable]] =
    ZIO.succeed(Server.port(config.port) ++ Server.paranoidLeakDetection ++ Server.app(app ++ staticApp))

}

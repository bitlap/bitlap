/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.http

import io.circe.generic.auto.exportEncoder
import io.circe.syntax.EncoderOps
import org.bitlap.common.utils.internal._
import org.bitlap.network._
import org.bitlap.server._
import org.bitlap.server.http.vo._
import zhttp.http._
import zhttp.service._
import zhttp.service.server.ServerChannelFactory
import zio._
import zio.console._

import java.sql._
import java.util.Properties
import scala.util._

/** bitlap http服务
 *
 *  初始化数据接口： http://localhost:8080/init
 *
 *  查询数据接口：http://localhost:8080/sql
 *  @param port
 */
final class HttpServerProvider(val port: Int) extends ServerProvider {

  Class.forName(classOf[org.bitlap.Driver].getName)

  val properties = new Properties()
  properties.put("bitlapconf:retries", "3")

  // TODO: 全局的异常处理 和 全局的响应包装对象
  private val app = Http.collectZIO[Request] {
    case req @ Method.POST -> !! / "api" / "sql" / "run" =>
      req.data.toJson.map { body =>
        val sql           = body.hcursor.get[String]("sql").getOrElse("")
        val conn          = DriverManager.getConnection("jdbc:bitlap://localhost:23333/default", properties)
        val stmt          = conn.createStatement()
        var rs: ResultSet = null
        try {
          stmt.execute(sql)
          rs = stmt.getResultSet

          val table = DBTablePrinter.from(rs)
          Response.json(s"""
                           |{
                           |  "success": true,
                           |  "data": ${SqlData.fromDBTable(table).asJson.noSpaces}
                           |}
                           |""".stripMargin)

        } catch {
          case e: Exception =>
            e.printStackTrace()
            Response.json(s"""
                             |{
                             |  "success": true,
                             |  "data": ${SqlData().asJson.noSpaces}
                             |}
                             |""".stripMargin)
        } finally {
          stmt.close()
          conn.close()
        }
      }
    case Method.GET -> !! / "api" / "common" / "status" => ZIO.effect(Response.json(s"""{"status":"ok"}"""))
  }

  private val indexHtml = Http.fromResource(s"static/index.html")
  private val staticApp = Http.collectHttp[Request] {
    case req
        if req.method == Method.GET
          && req.path.startsWith(!! / "pages") =>
      indexHtml
    case Method.GET -> !! / path => Http.fromResource(s"static/$path")
    case _ =>
      indexHtml
  }

  private val server = Server.port(port) ++ Server.paranoidLeakDetection ++ Server.app(app ++ staticApp)

  override def service(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    val nThreads: Int = args.headOption.flatMap(x => Try(x.toInt).toOption).getOrElse(0)
    (server.make
      .use(_ => putStrLn(s"$serverType: Server is listening to port: $port")) *> ZIO.never)
      .onInterrupt(putStrLn(s"$serverType: Server stopped").ignore)
      .provideCustomLayer(ServerChannelFactory.auto ++ EventLoopGroup.auto(nThreads))
      .exitCode
  }

  override def serverType: ServerType = ServerType.Http
}

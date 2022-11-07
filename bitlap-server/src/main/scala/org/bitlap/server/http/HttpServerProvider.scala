/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.http

import io.circe.generic.auto.exportEncoder
import io.circe.syntax.EncoderOps
import org.bitlap.common.jdbc._
import org.bitlap.server.ServerProvider
import zhttp.http._
import zhttp.service.server.ServerChannelFactory
import zhttp.service._
import zio._
import org.bitlap.network.ServerType
import zio.console.putStrLn

import java.sql.DriverManager
import scala.util.Try

/** bitlap http服务
 *
 *  初始化数据接口： http://localhost:8080/init
 *
 *  查询数据接口：http://localhost:8080/sql
 *  @param port
 */
final class HttpServerProvider(val port: Int) extends ServerProvider {

  private val table = "table_test"

  Class.forName(classOf[org.bitlap.Driver].getName)

  // 测试数据
  private def initTable(): Unit = {
    val stmt = getConn.createStatement()
    stmt.execute(s"create table if not exists $table")
    stmt.execute(s"load data 'classpath:simple_data.csv' overwrite table $table")
  }

  private def getConn = DriverManager.getConnection("jdbc:bitlap://localhost:23333/default")

  private val app = Http.collectZIO[Request] {
    case Method.GET -> !! / "init" => ZIO.effect(initTable()).as(Response.json("true"))
    case req @ Method.GET -> !! / "sql" =>
      ZIO.effect {
        val stmt = getConn.createStatement()
        stmt.execute(s"""
                      |select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv
                      |from $table
                      |where _time >= ${req.url.queryParams.getOrElse("_time", Nil).headOption.getOrElse(100)}
                      |group by _time
                      |""".stripMargin)
        val rs = stmt.getResultSet

        val ret = ResultSetTransformer[GenericRow4[Long, Double, Double, Long]].toResults(rs)
        Response.json(ret.asJson.noSpaces)
      }
    case Method.GET -> !! / "utc" => clock.currentDateTime.map(s => Response.text(s.toString))
  }

  private val server = Server.port(port) ++ Server.paranoidLeakDetection ++ Server.app(app)

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

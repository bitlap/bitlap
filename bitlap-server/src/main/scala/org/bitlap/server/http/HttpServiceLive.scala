/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.http

import io.circe.syntax.EncoderOps
import org.bitlap.common.utils.internal.DBTablePrinter
import org.bitlap.server.http.vo.SqlData
import zhttp.http.Response

import java.util.Properties
import java.sql._
import io.circe.generic.auto.exportEncoder
import zio._

/** @author
 *    梦境迷离
 *  @version 1.0,2023/3/13
 */
object HttpServiceLive {
  lazy val live: ULayer[Has[HttpServiceLive]] = ZLayer.succeed(new HttpServiceLive)
}
final class HttpServiceLive {

  Class.forName(classOf[org.bitlap.Driver].getName)

  val properties = new Properties()
  properties.put("bitlapconf:retries", "3")

  def execute(sql: String): Response = {
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

}

/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.http

import com.typesafe.scalalogging.LazyLogging
import org.bitlap.common.utils.internal.DBTablePrinter
import zio.*

import java.sql.*
import java.util.Properties

/** HTTP 具体逻辑实现
 *  @author
 *    梦境迷离
 *  @version 1.0,2023/3/13
 */
object HttpServiceLive:
  lazy val live: ULayer[HttpServiceLive] = ZLayer.succeed(new HttpServiceLive)
end HttpServiceLive

final class HttpServiceLive extends LazyLogging:

  Class.forName(classOf[org.bitlap.Driver].getName)

  val properties = new Properties()
  properties.put("bitlapconf:retries", "3")

  def execute(sql: String): SqlResult = {
    val conn          = DriverManager.getConnection("jdbc:bitlap://localhost:23333/default", properties)
    val stmt          = conn.createStatement()
    var rs: ResultSet = null
    try {
      stmt.execute(sql)
      rs = stmt.getResultSet
      val table = DBTablePrinter.from(rs)
      SqlResult(
        data = SqlData.fromDBTable(table),
        resultCode = 0
      )
    } catch {
      case e: Throwable =>
        logger.error("Executing sql error", e)
        SqlResult(
          data = SqlData.empty,
          errorMessage = e.getLocalizedMessage,
          resultCode = 1
        )
    } finally {
      stmt.close()
      conn.close()
    }
  }

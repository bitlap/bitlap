/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.http

import com.typesafe.scalalogging.LazyLogging
import dotty.tools.dotc.ast.Trees.ApplyKind.Using
import org.bitlap.common.utils.internal.DBTablePrinter
import zio.*

import java.sql.*
import java.util.Properties
import scala.util.{ Failure, Success, Try, Using as ScalaUtils }

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

  final val DEFAULT_URL = "jdbc:bitlap://localhost:23333/default"

  def execute(sql: String): SqlResult =
    ScalaUtils.resource(DriverManager.getConnection(DEFAULT_URL, properties)) { conn =>
      ScalaUtils.resource(conn.createStatement()) { stmt =>
        val executed = Try {
          stmt.execute(sql)
          val rs    = stmt.getResultSet
          val table = DBTablePrinter.from(rs)
          SqlResult(
            data = SqlData.fromDBTable(table),
            resultCode = 0
          )
        }
        executed match
          case Success(value) => value
          case Failure(exception) =>
            logger.error("Executing sql error", exception)
            SqlResult(
              data = SqlData.empty,
              errorMessage = exception.getLocalizedMessage,
              resultCode = 1
            )
      }
    }
  end execute

end HttpServiceLive

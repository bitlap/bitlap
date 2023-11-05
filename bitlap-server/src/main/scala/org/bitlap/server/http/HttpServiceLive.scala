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
package org.bitlap.server.http

import java.sql.*
import java.util.Properties

import scala.util.{ Failure, Success, Try, Using as ScalaUtils }

import org.bitlap.common.utils.internal.DBTablePrinter

import com.typesafe.scalalogging.LazyLogging

import dotty.tools.dotc.ast.Trees.ApplyKind.Using
import zio.*

/** HTTP Specific logic implementation
 */
object HttpServiceLive:
  lazy val live: ULayer[HttpServiceLive] = ZLayer.succeed(new HttpServiceLive)
end HttpServiceLive

final class HttpServiceLive extends LazyLogging:

  Class.forName(classOf[org.bitlap.Driver].getName)

  val properties = new Properties()
  properties.put("bitlapconf:retries", "3")

  private final val DEFAULT_URL = "jdbc:bitlap://localhost:23333/default"

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

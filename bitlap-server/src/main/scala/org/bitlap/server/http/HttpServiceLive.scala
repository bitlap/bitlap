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
import scala.util.control.NonFatal

import org.bitlap.common.utils.internal.DBTablePrinter
import org.bitlap.jdbc.Utils
import org.bitlap.network.{ BitlapResultSet as MyResultSet, _ }
import org.bitlap.network.{ ServerAddress, SyncConnection }

import com.typesafe.scalalogging.LazyLogging

import zio.*

/** HTTP Specific logic implementation
 */
object HttpServiceLive:
  lazy val live: ULayer[HttpServiceLive] = ZLayer.succeed(new HttpServiceLive)
end HttpServiceLive

final class HttpServiceLive extends LazyLogging:

  def execute(sql: String): SqlResult =
    var syncConnect: SyncConnection = null
    try {
      syncConnect = new SyncConnection("root", "")
      syncConnect.open(ServerAddress("localhost", 23333), 3000)

      val rss = Utils.getSqlStmts(sql.split("\n")).map { sql =>
        val rs = syncConnect.execute(sql)
        if (rs.hasNext)
          SqlResult(
            SqlData.fromList(MyResultSet.underlying(res.next(), res.tableSchema)),
            0
          )
        else
          SqlResult(
            SqlData.empty,
            0
          )
      }
      rss.lastOption.getOrElse(
        SqlResult(
          SqlData.empty,
          0
        )
      )
    } catch {
      case NonFatal(e) =>
        logger.error("Executing sql error", e)
        SqlResult(
          data = SqlData.empty,
          errorMessage = e.getLocalizedMessage,
          resultCode = 1
        )
    } finally {
      if (syncConnect != null) {
        syncConnect.close()
      }
    }

  end execute

end HttpServiceLive

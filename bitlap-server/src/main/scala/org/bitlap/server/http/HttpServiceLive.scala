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

import scala.util.control.NonFatal

import org.bitlap.common.utils.StringEx
import org.bitlap.network.*
import org.bitlap.server.BitlapGlobalContext

import com.typesafe.scalalogging.LazyLogging

import zio.*

/** HTTP Specific logic implementation
 */
object HttpServiceLive:

  lazy val live: ZLayer[BitlapGlobalContext, Nothing, HttpServiceLive] =
    ZLayer.fromFunction((in: BitlapGlobalContext) => new HttpServiceLive(in))
end HttpServiceLive

final class HttpServiceLive(context: BitlapGlobalContext) extends LazyLogging:

  def execute(sql: String): ZIO[Any, Throwable, SqlResult] =
    context.getSyncConnection.map { syncConnect =>
      try {
        syncConnect.open(ServerAddress(ProtocolConstants.Default_Host, ProtocolConstants.Port))
        val rss = StringEx.getSqlStmts(sql.split("\n").toList).map { sql =>
          syncConnect
            .execute(sql)
            .headOption
            .map { result =>
              SqlResult(
                SqlData.fromList(result.underlying),
                0
              )
            }
            .toList
        }
        rss.flatten.lastOption.getOrElse(
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
      }
    }

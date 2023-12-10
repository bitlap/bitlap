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
package org.bitlap.server.http.service

import org.bitlap.common.BitlapLogging
import org.bitlap.common.utils.StringEx
import org.bitlap.network.*
import org.bitlap.server.BitlapGlobalContext
import org.bitlap.server.http.model._

import zio.*

object SqlService {

  lazy val live: ZLayer[BitlapGlobalContext, Nothing, SqlService] =
    ZLayer.fromFunction((context: BitlapGlobalContext) => SqlService(context))
}

final class SqlService(context: BitlapGlobalContext) extends BitlapLogging {

  private val conf = context.config.grpcConfig

  def execute(sql: String): ZIO[Any, Throwable, SqlData] = {
    ZIO.acquireReleaseWith(context.getSyncConnection)(c => ZIO.attempt(c.close()).ignoreLogged) { conn =>
      conn.open(ServerAddress(conf.host, conf.port))
      ZIO.attempt {
        val rss = StringEx.getSqlStmts(sql).map { sql =>
          conn
            .execute(sql)
            .headOption
            .map { result =>
              SqlData.fromList(result.underlying)
            }
            .toList
        }
        rss.flatten.lastOption.getOrElse(
          SqlData.empty
        )
      }
    }
  }
}

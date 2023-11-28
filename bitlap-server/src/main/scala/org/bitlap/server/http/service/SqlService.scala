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

import scala.util.control.NonFatal

import org.bitlap.common.BitlapLogging
import org.bitlap.common.exception.{ BitlapExceptions, BitlapSQLException }
import org.bitlap.common.extension.*
import org.bitlap.common.utils.StringEx
import org.bitlap.common.utils.internal.DBTablePrinter
import org.bitlap.network.*
import org.bitlap.network.enumeration.TypeId
import org.bitlap.network.serde.BitlapSerde
import org.bitlap.server.BitlapGlobalContext
import org.bitlap.server.http.model.SqlData

import zio.*

object SqlService {

  lazy val live: ZLayer[BitlapGlobalContext, Nothing, SqlService] =
    ZLayer.fromFunction((context: BitlapGlobalContext) => SqlService(context))
}

class SqlService(context: BitlapGlobalContext) extends BitlapLogging {

  private val conf = context.config.grpcConfig

  def execute(sql: String): ZIO[Any, Throwable, SqlData] = {
    context.getSyncConnection.map(
      { syncConnect =>
        try {
          syncConnect.use { conn =>
            conn.open(ServerAddress(conf.host, conf.port))
            val rss = StringEx.getSqlStmts(sql.split("\n").toList).map { sql =>
              conn
                .execute(sql)
                .headOption
                .map { result =>
                  SqlData.fromList(underlying(result))
                }
                .toList
            }
            rss.flatten.lastOption.getOrElse(
              SqlData.empty
            )
          }
        } catch {
          case NonFatal(e) =>
            log.error("Execute sql failed", e)
            throw BitlapExceptions.httpException(-1, e.getMessage)
        }
      }
    )
  }

  private def underlying(result: Result): List[List[(String, String)]] = {
    if (result == null)
      throw BitlapSQLException("Without more elements, unable to get underlining of fetchResult")

    result.fetchResult.results.rows.map(_.values.zipWithIndex.map { case (string, i) =>
      val colDesc = result.tableSchema.columns.apply(i)
      colDesc.columnName -> {
        colDesc.typeDesc match
          case TypeId.DoubleType =>
            DBTablePrinter.normalizeValue(
              colDesc.typeDesc.value,
              colDesc.typeDesc.name,
              BitlapSerde.deserialize[Double](colDesc.typeDesc, string)
            )
          case _ =>
            DBTablePrinter.normalizeValue(
              colDesc.typeDesc.value,
              colDesc.typeDesc.name,
              BitlapSerde.deserialize[String](colDesc.typeDesc, string)
            )
      }
    }.toList)
  }
}

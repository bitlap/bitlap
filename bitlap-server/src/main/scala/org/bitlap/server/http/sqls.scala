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

import org.bitlap.common.exception.BitlapSQLException
import org.bitlap.common.utils.internal.DBTable
import org.bitlap.network.Result
import org.bitlap.network.serde.BitlapSerde

/** Wrapping sql data for queries
 */
final case class SqlInput(sql: String)

final case class SqlData(columns: Seq[SqlColumn] = Seq.empty, rows: Seq[SqlRow] = Seq.empty)

final case class SqlColumn(name: String)
final case class SqlRow(cells: Map[String, String] = Map.empty)
final case class SqlResult(data: SqlData, resultCode: Int, errorMessage: String = "")

object SqlData:

  def empty: SqlData = SqlData(Seq.empty, Seq.empty)

  def fromList(list: List[List[(String, String)]]): SqlData = {
    val columns = list.headOption.getOrElse(List.empty).map(c => SqlColumn(c._1))
    val sqlRows = list.map(r => SqlRow.apply(r.toMap))
    SqlData(columns, sqlRows)
  }

  def fromDBTable(table: DBTable): SqlData = {
    if table == null || table.columns.isEmpty then return SqlData()
    val columns = table.columns.map(_.label).map(SqlColumn.apply)
    val rows    = table.columns.map(_.values)
    val headRow = rows.head
    if headRow == null || headRow.isEmpty then {
      return SqlData(columns)
    }
    val sqlRows = headRow.indices.map { i =>
      val rs    = rows.map(_(i))
      val cells = rs.zipWithIndex.map { case (r, i) => columns(i).name -> r }.toMap
      SqlRow(cells)
    }
    SqlData(columns, sqlRows)
  }

extension (result: Result)

  def underlying: List[List[(String, String)]] = {
    if (result == null)
      throw BitlapSQLException("Without more elements, unable to get underlining of fetchResult")
    result.fetchResult.results.rows.map(_.values.zipWithIndex.map { case (string, i) =>
      val typeDesc = result.tableSchema.columns.apply(i).typeDesc
      typeDesc.name -> BitlapSerde.deserialize[String](typeDesc, string)
    }.toList)
  }

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

import scala.jdk.CollectionConverters.ListHasAsScala

import org.bitlap.common.utils.internal.DBTable

/** Wrapping sql data for queries
 */
final case class SqlInput(sql: String)

final case class SqlData(columns: Seq[SqlColumn] = Seq.empty, rows: Seq[SqlRow] = Seq.empty)

final case class SqlColumn(name: String)
final case class SqlRow(cells: Map[String, String] = Map.empty)
final case class SqlResult(data: SqlData, resultCode: Int, errorMessage: String = "")

object SqlData:

  def empty: SqlData = SqlData(Seq.empty, Seq.empty)

  def fromDBTable(table: DBTable): SqlData = {
    if table == null || table.getColumns.isEmpty then return SqlData()
    val columns = table.getColumns.asScala.map(_.getLabel).map(SqlColumn.apply).toSeq
    val rows    = table.getColumns.asScala.map(_.getValues)
    val headRow = rows.head.asScala
    if headRow == null || headRow.isEmpty then {
      return SqlData(columns)
    }
    val sqlRows = headRow.indices.map { i =>
      val rs    = rows.map(_.get(i))
      val cells = rs.zipWithIndex.map { case (r, i) => columns(i).name -> r }.toMap
      SqlRow(cells)
    }
    SqlData(columns, sqlRows)
  }

/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.http

import org.bitlap.common.utils.internal.DBTable

import scala.jdk.CollectionConverters.ListHasAsScala

/** Desc: sql 查询的数据封装
 */
final case class SqlInput(sql: String)

final case class SqlData(columns: Seq[SqlColumn] = Seq.empty, rows: Seq[SqlRow] = Seq.empty)

final case class SqlColumn(name: String)
final case class SqlRow(cells: Map[String, String] = Map.empty)
final case class SqlResult(
  data: SqlData,
  resultCode: Int,
  errorMessage: String = "Unknown Error"
)
object SqlData {

  def empty: SqlData = SqlData(Seq.empty, Seq.empty)

  def fromDBTable(table: DBTable): SqlData = {
    if (table == null) return SqlData()
    val columns = table.getColumns.asScala.map(_.getLabel).map(SqlColumn).toSeq
    val rows    = table.getColumns.asScala.map(_.getValues)
    val headRow = rows.head.asScala
    if (headRow == null || headRow.isEmpty) {
      return SqlData(columns)
    }
    val sqlRows = headRow.indices.map { i =>
      val rs    = rows.map(_.get(i))
      val cells = rs.zipWithIndex.map { case (r, i) => columns(i).name -> r }.toMap
      SqlRow(cells)
    }
    SqlData(columns, sqlRows)
  }
}

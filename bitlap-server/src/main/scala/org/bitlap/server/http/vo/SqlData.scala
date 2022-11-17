/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.http.vo

import org.bitlap.common.utils.internal.DBTable

import scala.jdk.CollectionConverters.ListHasAsScala

/**
 * Desc: sql 查询的数据封装
 */
case class SqlData(columns: Seq[SqlColumn] = Seq.empty, rows: Seq[SqlRow] = Seq.empty)

case class SqlColumn(name: String)
case class SqlRow(cells: Map[String, String] = Map.empty)

object SqlData {
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

/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.catalog.metadata

import scala.collection.mutable

/** Table metadata
 */
case class Table(
  val database: String,
  val name: String,
  val createTime: Long = System.currentTimeMillis(),
  var updateTime: Long = System.currentTimeMillis(),
  val props: mutable.Map[String, String] = mutable.Map(),
  // other fields
  val path: String) {

  import Table._

  override def toString: String = s"$database.$name"

  def getTableFormat: String = this.props(TABLE_FORMAT_KEY)
}

object Table {
  // table properties
  val TABLE_FORMAT_KEY = "table_format"
}

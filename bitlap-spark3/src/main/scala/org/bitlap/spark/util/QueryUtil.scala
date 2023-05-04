/* Copyright (c) 2023 bitlap.org */
package org.bitlap.spark.util

import com.google.common.base.Joiner
import com.google.common.collect.Iterables
import org.bitlap.spark.util.SchemaUtil.getEscapedFullColumnName

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.BufferHasAsJava
import scala.jdk.CollectionConverters.IterableHasAsJava

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/16
 */
object QueryUtil {

  def constructUpsertStatement(tableName: String, columns: List[String]): String = {
    if columns.isEmpty then throw new IllegalArgumentException("At least one column must be provided for upserts")
    val parameterList = ListBuffer[String]()
    var i             = 0
    while i < columns.size do {
      parameterList.append("?")

      i += 1
    }
    String.format(
      "UPSERT %s INTO %s (%s) VALUES (%s)",
      "",
      tableName,
      Joiner
        .on(", ")
        .join(Iterables.transform(columns.asJava, (columnName: String) => getEscapedFullColumnName(columnName))),
      Joiner.on(", ").join(parameterList.asJava)
    )
  }

}

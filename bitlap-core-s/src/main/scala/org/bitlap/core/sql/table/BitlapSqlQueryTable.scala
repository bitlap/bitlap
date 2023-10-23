/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.table

import java.util.{ Collections, List as JList }

import org.bitlap.common.exception.BitlapException
import org.bitlap.core.catalog.metadata.Table
import org.bitlap.core.sql.Keyword
import org.bitlap.core.sql.MDColumnAnalyzer
import org.bitlap.core.sql.QueryContext

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.`type`.RelDataTypeFactory
import org.apache.calcite.rex.RexNode
import org.apache.calcite.schema.ProjectableFilterableTable
import org.apache.calcite.schema.ScannableTable
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql.`type`.SqlTypeName

/** Desc: common bitlap table
 *
 *  Mail: chk19940609@gmail.com Created by IceMimosa Date: 2021/9/12
 */
class BitlapSqlQueryTable(val table: Table) extends AbstractTable with ProjectableFilterableTable with ScannableTable {

  lazy val analyzer: MDColumnAnalyzer =
    MDColumnAnalyzer(table, QueryContext.get().currentSelectNode) // must not be null

  /** Returns this table's row type.
   */
  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    val builder = typeFactory.builder()
    analyzer.getMetricColNames().foreach { it =>
      builder.add(it, SqlTypeName.ANY).nullable(true)
    }
    analyzer.getDimensionColNames().foreach { it =>
      if (it == Keyword.TIME) {
        builder.add(it, SqlTypeName.BIGINT)
      } else {
        builder.add(it, SqlTypeName.VARCHAR).nullable(true)
      }
    }
    return builder.build()
  }

  override def scan(root: DataContext, filters: JList[RexNode], projects: Array[Int]): Enumerable[Array[Any]] = {
    throw BitlapException(s"You need to implement this method in a subclass.")
  }

  override def scan(root: DataContext): Enumerable[Array[Any]] = {
    return this.scan(root, Collections.emptyList(), null)
  }

}

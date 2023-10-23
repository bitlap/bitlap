/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.table

import java.util.List as JList

import org.bitlap.core.catalog.metadata.Table

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.linq4j.Linq4j
import org.apache.calcite.rex.RexNode

/** empty table
 */
class BitlapSqlQueryEmptyTable(override val table: Table) extends BitlapSqlQueryTable(table) {

  override def scan(root: DataContext, filters: JList[RexNode], projects: Array[Int]): Enumerable[Array[Any]] = {
    return Linq4j.emptyEnumerable()
  }
}

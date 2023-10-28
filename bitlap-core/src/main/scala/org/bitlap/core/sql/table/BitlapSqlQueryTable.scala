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

/** common bitlap table
 */
class BitlapSqlQueryTable(val table: Table) extends AbstractTable with ProjectableFilterableTable with ScannableTable {

  lazy val analyzer: MDColumnAnalyzer =
    MDColumnAnalyzer(table, QueryContext.get().currentSelectNode) // must not be null

  /** Returns this table's row type.
   */
  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    val builder = typeFactory.builder()
    analyzer.metricColNames.foreach { it =>
      builder.add(it, SqlTypeName.ANY).nullable(true)
    }
    analyzer.dimensionColNames.foreach { it =>
      if (it == Keyword.TIME) {
        builder.add(it, SqlTypeName.BIGINT)
      } else {
        builder.add(it, SqlTypeName.VARCHAR).nullable(true)
      }
    }
    builder.build()
  }

  override def scan(root: DataContext, filters: JList[RexNode], projects: Array[Int]): Enumerable[Array[Any]] = {
    throw BitlapException(s"You need to implement this method in a subclass.")
  }

  override def scan(root: DataContext): Enumerable[Array[Any]] = {
    this.scan(root, Collections.emptyList(), null)
  }

}

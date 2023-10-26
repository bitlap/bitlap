/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.table

import java.util.List as JList

import scala.jdk.CollectionConverters.*

import org.bitlap.core.catalog.metadata.Table
import org.bitlap.core.mdm.FetchContext
import org.bitlap.core.mdm.plan.PendingFetchPlan
import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.PruneTimeFilter

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.linq4j.Linq4j
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.ImmutableIntList

class BitlapSqlQueryMetricTable(
  override val table: Table,
  private val timeFilter: PruneTimeFilter,
  private val pushedFilter: PrunePushedFilter)
    extends BitlapSqlQueryTable(table) {

  // filters is empty here, pushed by BitlapFilterTableScanRule
  override def scan(root: DataContext, filters: JList[RexNode], projects: Array[Int]): Enumerable[Array[Any]] = {
    if (projects == null) {
      // return Linq4j.emptyEnumerable()
      val rowType = super.getRowType(root.getTypeFactory)
      this.scan(root, ImmutableIntList.identity(rowType.getFieldCount).toIntArray)
    } else this.scan(root, projects)
  }

  private def scan(root: DataContext, projects: Array[Int]): Enumerable[Array[Any]] = {
    val rowType     = super.getRowType(root.getTypeFactory)
    val projections = projects.map { it => rowType.getFieldList.get(it).getName }.toList

    // add pending fetch plan to context
    val fetchContext = FetchContext(table, PendingFetchPlan(table, analyzer, timeFilter, pushedFilter))

    // find best plan to execute
    val bestPlan = fetchContext.findBestPlan()

    // execute the plan
    val rows = bestPlan.execute(fetchContext)

    Linq4j.asEnumerable(rows.toRows(projections).asJava)
  }
}

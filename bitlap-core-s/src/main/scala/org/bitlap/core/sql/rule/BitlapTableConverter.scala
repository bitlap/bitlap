/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.rule

import org.bitlap.core.extension._
import org.bitlap.core.sql.rel.BitlapTableFilterScan
import org.bitlap.core.sql.rel.BitlapTableScan
import org.bitlap.core.sql.table.BitlapSqlQueryEmptyTable
import org.bitlap.core.sql.table.BitlapSqlQueryMetricTable
import org.bitlap.core.sql.table.BitlapSqlQueryTable

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.prepare.RelOptTableImpl
import org.apache.calcite.rel.RelNode

import com.google.common.collect.ImmutableList

class BitlapTableConverter extends AbsRelRule(classOf[BitlapTableScan], "BitlapTableConverter") {

  override def convert0(_rel: RelNode, call: RelOptRuleCall): RelNode = {
    val rel = _rel.asInstanceOf[BitlapTableFilterScan]
    if (rel.converted) {
      return rel
    }
    val optTable = rel.getTable.asInstanceOf[RelOptTableImpl]
    val oTable   = optTable.table().asInstanceOf[BitlapSqlQueryTable]

    // convert to physical table scan
    val target =
      if (rel.isAlwaysFalse)
        BitlapSqlQueryEmptyTable(oTable.table)
      else
        BitlapSqlQueryMetricTable(oTable.table, rel.timeFilter, rel.pruneFilter)

    return rel
      .withTable(
        RelOptTableImpl.create(
          optTable.getRelOptSchema,
          optTable.getRowType,
          target,
          optTable.getQualifiedName.asInstanceOf[ImmutableList[String]]
        )
      )
      .also { it => it.converted = true }
  }
}

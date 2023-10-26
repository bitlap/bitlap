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

    rel
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

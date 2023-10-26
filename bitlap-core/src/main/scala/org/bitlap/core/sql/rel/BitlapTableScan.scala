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
package org.bitlap.core.sql.rel

import java.util.List as JList

import org.bitlap.core.sql.table.BitlapSqlQueryTable

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.prepare.RelOptTableImpl
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.hint.RelHint

/** Table scan logical plan, see [org.apache.calcite.rel.logical.LogicalTableScan]
 */
class BitlapTableScan(
  cluster: RelOptCluster,
  traitSet: RelTraitSet,
  hints: JList[RelHint],
  table: RelOptTable,
  var parent: RelNode = null)
    extends TableScan(cluster, traitSet, hints, table),
      BitlapNode {

  var converted: Boolean = false

  def getOTable: BitlapSqlQueryTable = {
    this.table.asInstanceOf[RelOptTableImpl].table().asInstanceOf[BitlapSqlQueryTable]
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super
      .explainTerms(pw)
      .item("class", getOTable.getClass.getSimpleName)
  }

  override def withHints(hintList: JList[RelHint]): RelNode = {
    BitlapTableScan(getCluster, getTraitSet, hintList, getTable, parent)
  }

  def withTable(oTable: RelOptTable): BitlapTableScan = {
    BitlapTableScan(getCluster, getTraitSet, getHints, oTable, parent)
  }

  override def copy(traitSet: RelTraitSet, inputs: JList[RelNode]): RelNode = {
    this
  }
}

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

import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.PruneTimeFilter

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.hint.RelHint

/** Bitlap table scan with push down filters
 */
class BitlapTableFilterScan(
  cluster: RelOptCluster,
  traitSet: RelTraitSet,
  hints: JList[RelHint],
  table: RelOptTable,
  val timeFilter: PruneTimeFilter,
  val pruneFilter: PrunePushedFilter,
  val isAlwaysFalse: Boolean,
  _parent: RelNode = null)
    extends BitlapTableScan(cluster, traitSet, hints, table, _parent) {

  override def explainTerms(pw: RelWriter): RelWriter = {
    super
      .explainTerms(pw)
      .item("timeFilter", timeFilter)
      .item("pruneFilter", pruneFilter)
  }

  override def withHints(hintList: JList[RelHint]): RelNode = {
    BitlapTableFilterScan(
      getCluster,
      getTraitSet,
      hintList,
      getTable,
      timeFilter,
      pruneFilter,
      isAlwaysFalse,
      parent
    )
  }

  override def withTable(oTable: RelOptTable): BitlapTableFilterScan = {
    BitlapTableFilterScan(
      getCluster,
      getTraitSet,
      getHints,
      oTable,
      timeFilter,
      pruneFilter,
      isAlwaysFalse,
      parent
    )
  }

  override def copy(traitSet: RelTraitSet, inputs: JList[RelNode]): RelNode = {
    this
  }
}

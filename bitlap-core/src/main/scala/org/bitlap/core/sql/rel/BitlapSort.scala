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

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rex.RexNode

/** Sort logical plan, see [org.apache.calcite.rel.logical.LogicalSort]
 */
class BitlapSort(
  cluster: RelOptCluster,
  traitSet: RelTraitSet,
  input: RelNode,
  collation: RelCollation,
  offset: RexNode,
  fetch: RexNode,
  var parent: RelNode = null)
    extends Sort(cluster, traitSet, input, collation, offset, fetch)
    with BitlapNode {

  override def copy(
    traitSet: RelTraitSet,
    newInput: RelNode,
    newCollation: RelCollation,
    offset: RexNode,
    fetch: RexNode
  ): Sort = {
    BitlapSort(getCluster, traitSet, newInput, newCollation, offset, fetch, parent)
  }
}

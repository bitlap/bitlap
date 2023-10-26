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

import java.util.Objects
import java.util.Set as JSet

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.core.CorrelationId
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.rex.RexNode

/** Filter logical plan, see [org.apache.calcite.rel.logical.LogicalFilter]
 */
class BitlapFilter(
  cluster: RelOptCluster,
  traitSet: RelTraitSet,
  child: RelNode,
  condition: RexNode,
  private val variablesSet: JSet[CorrelationId],
  var parent: RelNode = null)
    extends Filter(cluster, traitSet, child, condition)
    with BitlapNode {

  override def copy(traitSet: RelTraitSet, input: RelNode, condition: RexNode): Filter = {
    BitlapFilter(cluster, traitSet, input, condition, variablesSet, parent)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super
      .explainTerms(pw)
      .itemIf("variablesSet", variablesSet, !variablesSet.isEmpty)
  }

  override def deepEquals(obj: Any): Boolean = {
    deepEquals0(obj) &&
    variablesSet == obj.asInstanceOf[BitlapFilter].variablesSet
  }

  override def deepHashCode(): Int = {
    Objects.hash(deepHashCode0(), variablesSet)
  }

  override def getVariablesSet: JSet[CorrelationId] = {
    variablesSet
  }
}

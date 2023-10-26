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

import java.util.{ Collections, List as JList, Set as JSet }

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{ CorrelationId, Project }
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rex.RexNode

/** Project logical plan, see [org.apache.calcite.rel.logical.LogicalProject]
 */
class BitlapProject(
  cluster: RelOptCluster,
  traitSet: RelTraitSet,
  hints: JList[RelHint],
  input: RelNode,
  projects: JList[RexNode],
  rowType: RelDataType,
  private val _variablesSet: JSet[CorrelationId] = Collections.emptySet(),
  var parent: RelNode = null)
    extends Project(cluster, traitSet, hints, input, projects, rowType, _variablesSet)
    with BitlapNode {

  def this(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    hints: JList[RelHint],
    input: RelNode,
    projects: JList[RexNode],
    rowType: RelDataType,
    parent: RelNode
  ) = this(cluster, traitSet, hints, input, projects, rowType, Collections.emptySet(), parent)

  override def copy(
    traitSet: RelTraitSet,
    input: RelNode,
    projects: JList[RexNode],
    rowType: RelDataType
  ): BitlapProject = {
    BitlapProject(getCluster, traitSet, getHints, input, projects, rowType, getVariablesSet, parent)
  }

  override def withHints(hintList: JList[RelHint]): RelNode = {
    BitlapProject(
      getCluster,
      getTraitSet,
      hintList,
      getInput,
      getProjects,
      getRowType,
      getVariablesSet,
      parent
    )
  }

  override def deepHashCode(): Int = {
    super.deepHashCode0()
  }

  override def deepEquals(obj: Any): Boolean = {
    super.deepEquals0(obj)
  }

  def copy(input: RelNode, rowType: RelDataType, projects: JList[RexNode]): BitlapProject = {
    BitlapProject(getCluster, getTraitSet, getHints, input, projects, rowType, getVariablesSet, parent)
  }
}

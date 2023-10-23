/** Copyright (C) 2023 bitlap.org .
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
    return BitlapProject(getCluster, traitSet, getHints, input, projects, rowType, Collections.emptySet(), parent)
  }

  override def withHints(hintList: JList[RelHint]): RelNode = {
    return BitlapProject(
      getCluster,
      getTraitSet,
      hintList,
      getInput,
      getProjects,
      getRowType,
      Collections.emptySet(),
      null
    )
  }

  override def deepHashCode(): Int = {
    return super.deepHashCode0()
  }

  override def deepEquals(obj: Any): Boolean = {
    return super.deepEquals0(obj)
  }

  def copy(input: RelNode, rowType: RelDataType, projects: JList[RexNode]): BitlapProject = {
    return BitlapProject(getCluster, traitSet, getHints, input, projects, rowType, parent)
  }
}

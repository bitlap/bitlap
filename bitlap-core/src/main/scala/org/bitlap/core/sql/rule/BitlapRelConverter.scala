/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.rule

import scala.jdk.CollectionConverters._

import org.bitlap.core.extension._
import org.bitlap.core.sql.rel.BitlapAggregate
import org.bitlap.core.sql.rel.BitlapFilter
import org.bitlap.core.sql.rel.BitlapNode
import org.bitlap.core.sql.rel.BitlapProject
import org.bitlap.core.sql.rel.BitlapSort
import org.bitlap.core.sql.rel.BitlapTableScan
import org.bitlap.core.sql.rel.BitlapUnion
import org.bitlap.core.sql.rel.BitlapValues

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.SingleRel
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.calcite.rel.logical.LogicalFilter
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rel.logical.LogicalSort
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.rel.logical.LogicalUnion
import org.apache.calcite.rel.logical.LogicalValues
import org.joor.Reflect

/** Convert calcite rel node to bitlap rel node.
 */
class BitlapRelConverter extends AbsRelRule(classOf[RelNode], "BitlapRelConverter") {

  override def convert0(rel: RelNode, call: RelOptRuleCall): RelNode = {
    val root = call.getPlanner.getRoot.clean()
    // only convert root rel node
    if (rel != root) {
      return rel
    }
    this.convert00(rel, call)
  }

  private def convert00(_rel: RelNode, call: RelOptRuleCall): RelNode = {
    _rel match {
      // has been converted
      case rel: BitlapNode =>
        rel
      case rel: HepRelVertex =>
        // next parent should be HepRelVertex's parent and next level should be current level, because it's a wrapper
        rel.also { it =>
          Reflect.on(it).call("replaceRel", this.convert00(it.getCurrentRel, call))
        }
      case rel: LogicalSort =>
        this.convert00(rel.getInput, call).injectParent { it =>
          BitlapSort(rel.getCluster, rel.getTraitSet, it, rel.getCollation, rel.offset, rel.fetch)
        }
      case rel: LogicalAggregate =>
        this.convert00(rel.getInput, call).injectParent { it =>
          BitlapAggregate(
            rel.getCluster,
            rel.getTraitSet,
            rel.getHints,
            it,
            rel.getGroupSet,
            rel.getGroupSets,
            rel.getAggCallList
          )
        }
      case rel: LogicalProject =>
        this.convert00(rel.getInput, call).injectParent { it =>
          new BitlapProject(
            rel.getCluster,
            rel.getTraitSet,
            rel.getHints,
            it,
            rel.getProjects,
            rel.getRowType,
            rel.getVariablesSet
          )
        }
      case rel: LogicalFilter =>
        this.convert00(rel.getInput, call).injectParent { it =>
          BitlapFilter(rel.getCluster, rel.getTraitSet, it, rel.getCondition, rel.getVariablesSet)
        }
      case rel: LogicalUnion =>
        val union = BitlapUnion(rel.getCluster, rel.getTraitSet, rel.getInputs, rel.all)
        rel.getInputs.asScala.map { i =>
          this.convert00(i, call).injectParent { _ =>
            union
          }
        }.head
      case rel: LogicalTableScan =>
        BitlapTableScan(rel.getCluster, rel.getTraitSet, rel.getHints, rel.getTable)
      case rel: LogicalValues =>
        BitlapValues(rel.getCluster, rel.getTraitSet, rel.getRowType, rel.getTuples)
      case _ =>
        _rel match {
          case rel: SingleRel =>
            this.convert00(rel.getInput, call).injectParent { it =>
              rel.replaceInput(0, it)
              rel
            }
          case rel =>
            throw IllegalArgumentException(s"Invalid converted rel node: ${rel.getDigest}")
        }
    }
  }
}

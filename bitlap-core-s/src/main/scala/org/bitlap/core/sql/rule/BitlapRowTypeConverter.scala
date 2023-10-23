/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.rule

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*

import org.bitlap.core.sql.rel.BitlapAggregate
import org.bitlap.core.sql.rel.BitlapNode
import org.bitlap.core.sql.rel.BitlapProject

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexShuttle
import org.apache.calcite.sql.`type`.BasicSqlType

/** convert row any type to actually type from child node
 */
class BitlapRowTypeConverter extends AbsRelRule(classOf[BitlapNode], "BitlapRowTypeConverter") {

  override def convert0(_rel: RelNode, call: RelOptRuleCall): RelNode = {
    val rel = _rel.asInstanceOf[BitlapNode]
    // convert root node
    val parent = rel.parent
    if (parent != null || !hasAnyType(rel.getRowType)) {
      return rel
    }
    return this.convert00(rel, call)
  }

  private def convert00(_rel: RelNode, call: RelOptRuleCall): RelNode = {
    return _rel match {
      case rel: HepRelVertex => {
        this.convert00(rel.getCurrentRel, call)
      }
      case rel if !hasAnyType(rel.getRowType) => {
        rel.getInputs.asScala.zipWithIndex.foreach { case (n, i) =>
          rel.replaceInput(i, this.convert00(n, call))
        }
        rel
      }
      case rel: BitlapProject => {
        this.convertProjectType(rel, this.convert00(rel.getInput, call))
      }
      case rel: BitlapAggregate => {
        val i = this.convert00(rel.getInput, call)
        val agg = rel.getAggCallList.asScala.map { it =>
          AggregateCall.create(
            it.getAggregation,
            it.isDistinct,
            it.isApproximate,
            it.ignoreNulls(),
            it.getArgList,
            it.filterArg,
            it.distinctKeys,
            it.collation,
            rel.getGroupCount,
            i,
            null,
            it.name
          )
        }
        rel.copy(i, agg.asJava)
      }
      case rel => {
        rel.getInputs.asScala.zipWithIndex.foreach { case (n, i) =>
          rel.replaceInput(i, this.convert00(n, call))
        }
        rel
      }
    }
  }

  private def hasAnyType(rowType: RelDataType): Boolean = {
    return rowType match {
      case _: BasicSqlType => {
        rowType.getSqlTypeName.name().toUpperCase() == "ANY"
      }
      case _ => {
        rowType.getFieldList.asScala.exists(_.getType.getSqlTypeName.name().toUpperCase() == "ANY")
      }
    }
  }

  // convert project rowType and expr type
  private def convertProjectType(rel: BitlapProject, newInput: RelNode): BitlapProject = {
    val builder       = rel.getCluster.getTypeFactory.builder()
    val inputRowType  = rel.getInput.getRowType
    val outputRowType = rel.getRowType
    val newProjects = outputRowType.getFieldList.asScala.zipWithIndex.map { case (rt, idx) =>
      val refIndex = ListBuffer[Int]()
      val newProject = rel.getProjects
        .get(idx)
        .accept(new RexShuttle() {
          override def visitInputRef(inputRef: RexInputRef): RexNode = {
            refIndex += inputRef.getIndex
            val newRef =
              if (hasAnyType(inputRef.getType))
                RexInputRef.of(inputRef.getIndex, inputRowType)
              else
                inputRef
            return super.visitInputRef(newRef)
          }
        })
      if (hasAnyType(rt.getType) && refIndex.nonEmpty) {
        builder.add(inputRowType.getFieldList.get(refIndex.head))
      } else {
        builder.add(rt)
      }
      newProject
    }
    return rel.copy(newInput, builder.build(), newProjects.asJava)
  }
}

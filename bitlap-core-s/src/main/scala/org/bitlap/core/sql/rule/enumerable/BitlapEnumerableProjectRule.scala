/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.rule.enumerable

import org.bitlap.core.sql.rel.BitlapProject
import org.bitlap.core.sql.rule.AbsRelRule

import org.apache.calcite.adapter.enumerable.EnumerableConvention
import org.apache.calcite.adapter.enumerable.EnumerableProject
import org.apache.calcite.adapter.enumerable.EnumerableRules
import org.apache.calcite.plan.{ Convention, RelOptPlanner, RelOptRule, RelOptRuleCall, RelTrait, RelTraitDef }
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.Project

import com.google.common.base.Predicate

/** Convert BitlapProject to enumerable rule.
 *
 *  @see
 *    [org.apache.calcite.adapter.enumerable.EnumerableProjectRule]
 *  @see
 *    [EnumerableRules.ENUMERABLE_PROJECT_RULE]
 */
class BitlapEnumerableProjectRule extends AbsRelRule(BitlapEnumerableProjectRule.CONFIG) {

  override def convert0(rel: RelNode, call: RelOptRuleCall): RelNode = {
    val project = rel.asInstanceOf[Project]
    return EnumerableProject.create(
      RelOptRule.convert(
        project.getInput,
        project.getInput.getTraitSet.replace(EnumerableConvention.INSTANCE)
      ),
      project.getProjects,
      project.getRowType
    )
  }
}

object BitlapEnumerableProjectRule {

  val CONFIG = ConverterRule.Config.INSTANCE.withConversion(
    classOf[BitlapProject],
    (input: BitlapProject) => !input.containsOver(),
    Convention.NONE,
    EnumerableConvention.INSTANCE,
    "BitlapEnumerableProjectRule"
  )
}

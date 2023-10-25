/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.rule.enumerable

import org.bitlap.core.sql.rel.BitlapProject
import org.bitlap.core.sql.rule.AbsRelRule

import org.apache.calcite.adapter.enumerable.{ EnumerableConvention, EnumerableProject }
import org.apache.calcite.plan.{ Convention, RelOptRule, RelOptRuleCall }
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.Project

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
    EnumerableProject.create(
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

  val CONFIG: ConverterRule.Config = ConverterRule.Config.INSTANCE.withConversion(
    classOf[BitlapProject],
    (input: BitlapProject) => !input.containsOver(),
    Convention.NONE,
    EnumerableConvention.INSTANCE,
    "BitlapEnumerableProjectRule"
  )
}

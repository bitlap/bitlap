/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql

import org.bitlap.core.sql.rel.BitlapNode
import org.bitlap.core.sql.rule.enumerable.BitlapEnumerableAggregateRule
import org.bitlap.core.sql.rule.enumerable.BitlapEnumerableAggregateSortedRule
import org.bitlap.core.sql.rule.enumerable.BitlapEnumerableFilterRule
import org.bitlap.core.sql.rule.enumerable.BitlapEnumerableProjectRule
import org.bitlap.core.sql.rule.enumerable.BitlapEnumerableUnionMergeRule
import org.bitlap.core.sql.rule.enumerable.BitlapEnumerableUnionRule

import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.rules.CoreRules

/** Entrance functions for sql rules.
 */

package object rule {

  /** @see
   *    [org.apache.calcite.rel.rules.CoreRules]
   */
  val RULES = List(
    // rules to transform calcite logical node plan
    List(CoreRules.UNION_MERGE),
    // rules to transform bitlap node plan
    List(
      BitlapRelConverter(),
      BitlapAggConverter(),
      BitlapFilterTableScanRule(),
      ValidRule(),
      BitlapTableConverter()
    ),
    List(BitlapRowTypeConverter())
  )

  /** @see
   *    [org.apache.calcite.tools.Programs.RULE_SET]
   *  @see
   *    [org.apache.calcite.adapter.enumerable.EnumerableRules.ENUMERABLE_RULES]
   */
  val ENUMERABLE_RULES = List(
    BitlapEnumerableAggregateRule(),
    BitlapEnumerableAggregateSortedRule(),
    BitlapEnumerableProjectRule(),
    BitlapEnumerableFilterRule(),
    BitlapEnumerableUnionRule(),
    BitlapEnumerableUnionMergeRule()
  )

  extension (relNode: RelNode) {

    /** clean HepRelVertex wrapper
     */
    def clean(): RelNode = {
      return relNode match
        case r: HepRelVertex => r.getCurrentRel
        case _               => relNode
    }

    /** inject parent node
     *
     *  @return
     *    parent node
     */
    def injectParent(parent: (RelNode) => RelNode): RelNode = {
      val p = parent(relNode)
      relNode match {
        case node: BitlapNode =>
          node.parent = p
        case r if r.isInstanceOf[HepRelVertex] && r.asInstanceOf[HepRelVertex].getCurrentRel.isInstanceOf[BitlapNode] =>
          r.asInstanceOf[HepRelVertex].getCurrentRel.asInstanceOf[BitlapNode].parent = p
      }
      return p
    }

  }
}

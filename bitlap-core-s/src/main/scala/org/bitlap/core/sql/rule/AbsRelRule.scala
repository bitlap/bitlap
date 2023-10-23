/** Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.rule

import scala.jdk.CollectionConverters._

import org.apache.calcite.plan.Convention
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.TableScan
import org.slf4j.{ Logger, LoggerFactory }

abstract class AbsRelRule(config: ConverterRule.Config) extends ConverterRule(config) {

  protected val log: Logger = LoggerFactory.getLogger(this.getClass)

  def this(input: Class[_ <: RelNode], description: String) = this(
    ConverterRule.Config.INSTANCE.withConversion(input, Convention.NONE, Convention.NONE, description)
  )

  override def onMatch(call: RelOptRuleCall): Unit = {
    val rel = call.rel[RelNode](0)
    if (rel.getTraitSet.contains(getInTrait)) {
      val converted = convert0(rel, call)
      if (converted != null) {
        call.transformTo(converted)
      }
    }
  }

  protected def convert0(rel: RelNode, call: RelOptRuleCall): RelNode

  final override def convert(rel: RelNode): RelNode = {
    throw NotImplementedError(s"Deprecated by function convert0(RelNode, RelOptRuleCall).")
  }

  protected def hasTableScanNode(rel: RelNode): Boolean = {
    if (rel.isInstanceOf[TableScan]) {
      return true
    }
    return rel.getInputs.asScala.map(_.clean()).filter(_ != null).exists(this.hasTableScanNode)
  }
}

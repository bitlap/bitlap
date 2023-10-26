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
    rel.getInputs.asScala.map(_.clean()).filter(_ != null).exists(this.hasTableScanNode)
  }
}

/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.rule.shuttle

import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexShuttle

class RexInputRefShuttle private (val call: RexNode) extends RexShuttle {

  private val refs = scala.collection.mutable.Set[RexInputRef]()

  def getInputRefs: List[RexInputRef] = {
    refs.toList
  }

  private def init(): RexInputRefShuttle = {
    this.call.accept(this)
    this
  }

  override def visitInputRef(inputRef: RexInputRef): RexNode = {
    refs.add(inputRef)
    super.visitInputRef(inputRef)
  }

}

object RexInputRefShuttle {

  def of(call: RexNode): RexInputRefShuttle = {
    RexInputRefShuttle(call).init()
  }
}

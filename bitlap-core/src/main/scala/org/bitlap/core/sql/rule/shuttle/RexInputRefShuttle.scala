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

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
package org.bitlap.core.mdm.model

import java.io.Serializable

import org.bitlap.common.utils.PreConditions

/** wrapper a cell metric value
 *
 *  * 0: distinct count (Long) * 1: count (Long) * 2: sum (Double)
 */
open class RowValueMeta extends Serializable {

  private val values: Array[Number] = Array(0L, 0L, 0.0)

  def add0(v: Number): RowValueMeta = {
    this.values(0) = v.longValue() + this.values(0).longValue()
    this
  }

  def add1(v: Number): RowValueMeta = {
    this.values(1) = v.longValue() + this.values(1).longValue()
    this
  }

  def add2(v: Number): RowValueMeta = {
    this.values(2) = v.doubleValue() + this.values(2).doubleValue()
    this
  }
  def add(v0: Number, v1: Number, v2: Number): RowValueMeta = this.add0(v0).add1(v1).add2(v2)
  def add(v: RowValueMeta): RowValueMeta                    = this.add0(v(0)).add1(v(1)).add2(v(2))

  def apply(idx: Int): Number = {
    PreConditions.checkExpression(idx >= 0 && idx <= 2)
    this.values(idx)
  }

  private def canEqual(other: Any): Boolean = other.isInstanceOf[RowValueMeta]

  override def equals(other: Any): Boolean = {
    other match
      case that: RowValueMeta =>
        that.canEqual(this) &&
        (values sameElements that.values)
      case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(values)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = {
    s"[v0=${this(0)}, v1=${this(1)}, v2=${this(2)}]"
  }

}

object RowValueMeta {
  def empty(): RowValueMeta                            = RowValueMeta()
  def of(v0: Long, v1: Long, v2: Double): RowValueMeta = RowValueMeta().add0(v0).add1(v1).add2(v2)
}

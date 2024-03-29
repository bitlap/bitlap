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
package org.bitlap.core.mdm.plan

import scala.collection.mutable

import org.bitlap.common.BitlapIterator
import org.bitlap.common.utils.PreConditions
import org.bitlap.core.mdm.FetchContext
import org.bitlap.core.mdm.FetchPlan
import org.bitlap.core.mdm.format.DataType
import org.bitlap.core.mdm.format.DataTypes
import org.bitlap.core.mdm.model.Row
import org.bitlap.core.mdm.model.RowIterator
import org.bitlap.core.mdm.model.RowValueMeta
import org.bitlap.roaringbitmap.x.BM

/** Merge metrics with same dimensions into a single row
 */
class MetricMergeFetchPlan(override val subPlans: List[FetchPlan]) extends FetchPlan {

  override def execute(context: FetchContext): RowIterator = {
    if (this.subPlans.isEmpty) {
      return RowIterator.empty()
    }
    if (this.subPlans.size == 1) {
      return this.subPlans.head.execute(context)
    }
    val rowsSet = this.subPlans.map(_.execute(context)) // TODO: par

    // reset key and value types from 0, 1, 2, ...
    val resultKeyTypes = this.getKeyTypes(rowsSet).zipWithIndex.map { case (dt, idx) =>
      DataTypes.resetIndex(dt, idx)
    }
    val resultValueTypes = this.getValueTypes(rowsSet).zipWithIndex.map { case (dt, idx) =>
      DataTypes.resetIndex(dt, resultKeyTypes.size + idx)
    }

    // merge values with same the keys
    val results = new mutable.LinkedHashMap[List[Any], Row]()
    var offset  = 0
    for (rs <- rowsSet) {
      // get actual types from inputs
      val keyTypes   = rs.getTypes(resultKeyTypes.map(_.name))
      val valueTypes = rs.valueTypes
      rs.rows.foreach { row =>
        val keys = keyTypes.map(dt => row(dt))
        if (results.contains(keys)) {
          val r = results(keys)
          valueTypes.zipWithIndex.foreach { case (vt, idx) =>
            val i    = idx + offset + keys.size
            val cell = r(i)
            // merge cells
            if (cell == null) {
              r(i) = row(vt.idx)
            } else {
              cell match {
                case bm: BM =>
                  r(i) = bm.or(row(vt.idx).asInstanceOf[BM])
                case meta: RowValueMeta =>
                  r(i) = meta.add(row(vt.idx).asInstanceOf[RowValueMeta])
                case _ =>
                  throw IllegalArgumentException(s"Illegal input types: ${cell.getClass}")
              }
            }
          }
        } else {
          val r = Row(resultKeyTypes.size + resultValueTypes.size)
          keys.zipWithIndex.foreach { case (key, idx) =>
            r(idx) = key
          }
          valueTypes.zipWithIndex.foreach { case (vt, idx) =>
            r(idx + offset + keys.size) = row(vt.idx)
          }
          results(keys) = r
        }
      }
      offset += valueTypes.size
    }
    RowIterator(BitlapIterator.of(results.values), resultKeyTypes, resultValueTypes)
  }

  private def getKeyTypes(rowsSet: List[RowIterator]): List[DataType] = {
    PreConditions.checkNotEmpty(rowsSet)
    val keyNames = rowsSet.map { r => r.keyTypes.map(_.name).sorted }.reduce { case (a, b) =>
      PreConditions.checkExpression(
        a == b,
        "",
        s"Row iterators key types need to be the same, one is $a, the other is $b"
      )
      a
    }
    rowsSet.head.getTypes(keyNames)
  }

  private def getValueTypes(rowsSet: List[RowIterator]): List[DataType] = {
    PreConditions.checkNotEmpty(rowsSet)
    rowsSet.map { r => r.valueTypes }.reduce { case (a, b) => a ++ b }
  }

  override def explain(depth: Int): String = {
    s"${" ".repeat(depth)}+- MetricMergeFetchPlan\n${this.subPlans.map(_.explain(depth + 2)).mkString("\n")}"
  }
}

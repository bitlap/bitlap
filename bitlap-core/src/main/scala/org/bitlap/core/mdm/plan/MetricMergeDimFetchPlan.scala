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
import scala.jdk.CollectionConverters._

import org.bitlap.common.BitlapIterator
import org.bitlap.common.bitmap.BM
import org.bitlap.common.utils.BMUtils
import org.bitlap.common.utils.PreConditions
import org.bitlap.core.mdm.FetchContext
import org.bitlap.core.mdm.FetchPlan
import org.bitlap.core.mdm.format.DataTypeCBM
import org.bitlap.core.mdm.format.DataTypes
import org.bitlap.core.mdm.model.Row
import org.bitlap.core.mdm.model.RowIterator
import org.bitlap.core.sql.Keyword

/** Merge metrics with different dimensions into a single row
 */
class MetricMergeDimFetchPlan(override val subPlans: List[FetchPlan]) extends FetchPlan {

  override def execute(context: FetchContext): RowIterator = {
    if (this.subPlans.isEmpty) {
      return RowIterator.empty()
    }
    if (this.subPlans.size == 1) {
      return this.subPlans.last.execute(context)
    }
    val rowsSet = this.subPlans.map(_.execute(context)) // TODO: par

    rowsSet.reduce { case (rs1, rs2) => merge0(rs1, rs2) }
  }

  private def merge0(rs1: RowIterator, rs2: RowIterator): RowIterator = {
    // check key types should be different
    val keyTypes1 = rs1.keyTypes.map(_.name).filter(_ != Keyword.TIME)
    val keyTypes2 = rs2.keyTypes.map(_.name).filter(_ != Keyword.TIME)
    PreConditions.checkExpression(
      keyTypes1.intersect(keyTypes2).isEmpty,
      "",
      s"Row iterators key types need to be different, one is $keyTypes1, the other is $keyTypes2"
    )
    // check value types should be the same
    val valueTypes1 = rs1.valueTypes.map(_.name).sorted
    val valueTypes2 = rs2.valueTypes.map(_.name).sorted
    PreConditions.checkExpression(
      valueTypes1 == valueTypes2,
      "",
      s"Row iterators value types need to be the same, one is $valueTypes1, the other is $valueTypes2"
    )

    val resultKeyTypes = (rs1.keyTypes ++ rs2.keyTypes.filter(_.name != Keyword.TIME)).zipWithIndex.map {
      case (dt, idx) =>
        DataTypes.resetIndex(dt, idx)
    }
    val resultValueTypes = rs1.valueTypes.zipWithIndex.map { case (dt, idx) =>
      DataTypes.resetIndex(dt, resultKeyTypes.size + idx)
    }

    // let rows1 as cartesian or not
    val results = mutable.LinkedHashMap[List[Any], Row]()
    var rows1   = rs1
    var rows2   = rs2
    if (rs2.valueTypes.exists(_.isInstanceOf[DataTypeCBM])) {
      rows1 = rs2
      rows2 = rs1
    }

    // let rows2 as Join Build Table
    // TODO (consider sort merge join)
    val buffer = rows2.rows.asScala.toList.groupBy(_(0))
    for (r1 <- rows1.asScala) {
      val keys1   = r1.getByTypes(rows1.keyTypes)
      val tmValue = keys1.head
      if (buffer.contains(tmValue)) {
        for (r2 <- buffer(tmValue)) {
          val keys2 = r2.getByTypes(rows2.keyTypes)
          val rKeys = keys1 ++ keys2.drop(1)
          if (results.contains(rKeys)) {
            // should never get here
          } else {
            val cells = resultValueTypes.map { dt =>
              val cell1 = r1(rows1.getType(dt.name).idx)
              val cell2 = r2(rows2.getType(dt.name).idx)
              BMUtils.javaAnd(cell1.asInstanceOf[BM], cell2.asInstanceOf[BM])
            }
            results(rKeys) = Row((rKeys ++ cells).toArray)
          }
        }
      } else {
        val rKeys = keys1 ++ Array.fill[Any](rows2.keyTypes.size - 1)(null)
        val cells = resultValueTypes.map { dt => r1(rows1.getType(dt.name).idx) }
        results(rKeys) = Row((rKeys ++ cells).toArray)
      }
    }
    RowIterator(BitlapIterator.of(results.values.asJava), resultKeyTypes, resultValueTypes)
  }

  override def explain(depth: Int): String = {
    s"${" ".repeat(depth)}+- MetricMergeDimFetchPlan\n${this.subPlans.map(_.explain(depth + 2)).mkString("\n")}"
  }
}

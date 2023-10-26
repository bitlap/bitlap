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
package org.bitlap.core.sql.udf.aggr

import org.bitlap.common.bitmap.CBM
import org.bitlap.core.mdm.model.RowValueMeta
import org.bitlap.core.sql.infer
import org.bitlap.core.sql.udf.UDAF
import org.bitlap.core.sql.udf.UDFNames

import org.apache.calcite.sql.`type`.SqlReturnTypeInference
import org.apache.calcite.sql.`type`.SqlTypeName

/** compute sum metric from bitmap or metadata.
 */
class BMSumAggr extends UDAF[Number, Any, Number] {

  override val name: String                       = UDFNames.bm_sum_aggr
  override val inputTypes: List[SqlTypeName]      = List(SqlTypeName.ANY)
  override val resultType: SqlReturnTypeInference = SqlTypeName.DOUBLE.infer()

  override def init(): Number = 0.0

  override def add(accumulator: Number, input: Any): Number = {
    input match {
      case meta: RowValueMeta =>
        accumulator.doubleValue() + meta(2).doubleValue()
      case cbm: CBM =>
        accumulator.doubleValue() + cbm.getCount
      case _ =>
        throw IllegalArgumentException(s"Invalid input type: ${input.getClass}")
    }
  }

  override def merge(accumulator1: Number, accumulator2: Number): Number =
    accumulator1.doubleValue() + accumulator2.doubleValue()

  override def result(accumulator: Number): Number = accumulator
}

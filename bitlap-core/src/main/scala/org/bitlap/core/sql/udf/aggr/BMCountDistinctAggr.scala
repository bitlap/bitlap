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

import org.bitlap.core.mdm.model.RowValueMeta
import org.bitlap.core.sql.infer
import org.bitlap.core.sql.udf.UDAF
import org.bitlap.core.sql.udf.UDFNames
import org.bitlap.roaringbitmap.x.BM
import org.bitlap.roaringbitmap.x.RBM

import org.apache.calcite.sql.`type`.SqlReturnTypeInference
import org.apache.calcite.sql.`type`.SqlTypeName

/** compute count metric from bitmap or metadata.
 */
class BMCountDistinctAggr extends UDAF[(Number, BM), Any, Number] {

  override val name: String                       = UDFNames.bm_count_distinct
  override val inputTypes: List[SqlTypeName]      = List(SqlTypeName.ANY)
  override val resultType: SqlReturnTypeInference = SqlTypeName.BIGINT.infer()

  override def init(): (Number, BM) = Long.box(0L) -> RBM()

  override def add(accumulator: (Number, BM), input: Any): (Number, BM) = {
    val (l, r) = accumulator
    input match {
      case meta: RowValueMeta =>
        Long.box(l.longValue() + meta(0).longValue()) -> r
      case bm: BM =>
        l -> r.or(bm)
      case _ =>
        throw IllegalArgumentException(s"Invalid input type: ${input.getClass}")
    }
  }

  override def merge(accumulator1: (Number, BM), accumulator2: (Number, BM)): (Number, BM) = {
    val (l1, r1) = accumulator1
    val (l2, r2) = accumulator2
    Long.box(l1.longValue() + l2.longValue()) -> (r1.or(r2))
  }

  override def result(accumulator: (Number, BM)): Number = {
    val (l, r) = accumulator
    val result = r.getCountUnique
    if (result != 0L) Long.box(result) else l
  }
}

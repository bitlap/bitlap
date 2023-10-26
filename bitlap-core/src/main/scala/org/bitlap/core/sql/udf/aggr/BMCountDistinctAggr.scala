/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.udf.aggr

import org.bitlap.common.bitmap.BM
import org.bitlap.common.bitmap.RBM
import org.bitlap.core.mdm.model.RowValueMeta
import org.bitlap.core.sql.infer
import org.bitlap.core.sql.udf.UDAF
import org.bitlap.core.sql.udf.UDFNames

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
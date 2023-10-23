/**
 * Copyright (C) 2023 bitlap.org .
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
    return input match {
      case meta: RowValueMeta => {
        accumulator.doubleValue() + meta(2).doubleValue()
      }
      case cbm: CBM => {
        accumulator.doubleValue() + cbm.getCount
      }
      case _ => {
        throw IllegalArgumentException(s"Invalid input type: ${input.getClass}")
      }
    }
  }

  override def merge(accumulator1: Number, accumulator2: Number): Number =
    accumulator1.doubleValue() + accumulator2.doubleValue()

  override def result(accumulator: Number): Number = accumulator
}

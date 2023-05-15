/* Copyright (c) 2023 bitlap.org */
package org.bitlap.spark.writer

import org.bitlap.spark.{ BitlapOptions, SparkUtils }

import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.sources.InsertableRelation

/** Desc: V1 writer for Bitlap spark data
 */
class BitlapInsertableRelation(val writeInfo: LogicalWriteInfo, val options: BitlapOptions) extends InsertableRelation {

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val validSchema = SparkUtils.validSchema
    if (validSchema != data.schema) {
      throw new SparkException(
        s"Input schema is not valid. Input schema is ${data.schema}, valid schema is $validSchema"
      )
    }
    new BitlapMdmWriter(data, options).write()
  }
}

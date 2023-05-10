package org.bitlap.spark.writer

import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.sources.InsertableRelation
import org.bitlap.spark.SparkUtils

/**
 * Desc: V1 writer for Bitlap spark data
 */
class BitlapInsertableRelation extends InsertableRelation {
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {

    data.show(false)

    val spark = data.sparkSession

    if (SparkUtils.VALID_INPUT_SCHEMA != data.schema) {
      throw new SparkException(s"Input schema is not valid. Input schema is ${data.schema}, valid schema is ${SparkUtils.VALID_INPUT_SCHEMA}}")
    }



//    data.write.text()


    ???
  }
}

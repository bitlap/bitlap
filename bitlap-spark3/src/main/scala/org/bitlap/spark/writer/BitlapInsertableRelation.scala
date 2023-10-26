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
package org.bitlap.spark.writer

import org.bitlap.spark.{ BitlapOptions, SparkUtils }

import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.sources.InsertableRelation

/** V1 writer for Bitlap spark data
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

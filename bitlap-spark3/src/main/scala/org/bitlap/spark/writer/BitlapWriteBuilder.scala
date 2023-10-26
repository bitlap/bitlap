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

import org.bitlap.spark.BitlapOptions

import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.sources.{ Filter, InsertableRelation }

final class BitlapWriteBuilder(val writeInfo: LogicalWriteInfo, val options: BitlapOptions)
    extends WriteBuilder
    with SupportsOverwrite
    with SupportsDynamicOverwrite {

  override def build(): Write = new V1Write {
    override def toBatch: BatchWrite = super.toBatch

    override def toStreaming: StreamingWrite = super.toStreaming

    override def toInsertableRelation: InsertableRelation = {
      new BitlapInsertableRelation(writeInfo, options)
    }
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = this

  override def overwriteDynamicPartitions(): WriteBuilder = this
}

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
package org.bitlap.spark

import java.util.{ Map => JMap }

import scala.jdk.CollectionConverters.MapHasAsScala

import org.apache.spark.sql.connector.catalog.{ Table, TableProvider }
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

final class BitlapDataSource extends TableProvider with DataSourceRegister {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = SparkUtils.validSchema

  override def supportsExternalMetadata(): Boolean = true

  override def getTable(schema: StructType, transforms: Array[Transform], properties: JMap[String, String]): Table = {
    val options = properties.asScala ++ Map("driver" -> "org.bitlap.Driver")
    new BitlapTable(schema, transforms: Array[Transform], new BitlapOptions(options.toMap))
  }

  override def shortName(): String = "bitlap"
}

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
package org.bitlap.core.storage

import org.bitlap.core.catalog.metadata.Table
import org.bitlap.core.storage.io.BitlapParquetProvider

import org.apache.hadoop.fs.FileSystem

/** table format implementation.
 */
enum TableFormat(val name: String) {

  /** default implementation
   */
  case PARQUET extends TableFormat("parquet")

  def getProvider(table: Table, fs: FileSystem): TableFormatProvider = {
    name match
      case "parquet" => BitlapParquetProvider(table, fs)
      case _         => throw NotImplementedError()
  }
}

object TableFormat {

  def fromTable(table: Table): TableFormat =
    TableFormat.values.find(_.name == table.getTableFormat).get
}

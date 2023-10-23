package org.bitlap.core.storage

import org.apache.hadoop.fs.FileSystem
import org.bitlap.core.catalog.metadata.Table
import org.bitlap.core.storage.io.BitlapParquetProvider

/**
 * table format implemtation.
 */
enum TableFormat(val name: String) {
  /**
   * default implementation
   */
  case PARQUET extends TableFormat("parquet")

  def getProvider(table: Table, fs: FileSystem): TableFormatProvider = {
    name match
      case "parquet" => BitlapParquetProvider(table, fs)
      case _ => throw NotImplementedError()
  }
}

object TableFormat {
  def fromTable(table: Table) = TableFormat.valueOf(table.getTableFormat)
}

/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.storage

import org.apache.hadoop.fs.FileSystem
import org.bitlap.core.catalog.metadata.Table
import org.bitlap.core.storage.io.BitlapParquetProvider

/**
 * table format implemtation.
 */
enum class TableFormat {

    /**
     * default implementation
     */
    PARQUET {
        override fun getProvider(table: Table, fs: FileSystem): TableFormatProvider {
            return BitlapParquetProvider(table, fs)
        }
    },

    ;

    abstract fun getProvider(table: Table, fs: FileSystem): TableFormatProvider

    companion object {
        fun fromTable(table: Table) = TableFormat.valueOf(table.getTableFormat())
    }
}

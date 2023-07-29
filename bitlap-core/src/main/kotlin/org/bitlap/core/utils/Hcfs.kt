/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.bitlap.common.utils.JsonEx.json
import org.bitlap.common.utils.JsonEx.jsonAs
import org.bitlap.core.catalog.metadata.Table

/**
 * Hadoop Compatible File System common utils
 */
object Hcfs {

    /**
     * Write bitlap table schema.
     */
    fun FileSystem.writeTable(tableDir: Path, table: Table): Boolean {
        this.create(Path(tableDir, ".table"), true).use {
            it.writeUTF(table.json())
        }
        return true
    }

    /**
     * Read bitlap table schema.
     */
    fun FileSystem.readTable(tableDir: Path): Table {
        return this.open(Path(tableDir, ".table")).use {
            it.readUTF().jsonAs<Table>()
        }
    }

    /**
     * clone conf
     */
    fun FileSystem.newConf(): Configuration {
        return Configuration(this.conf)
    }
}

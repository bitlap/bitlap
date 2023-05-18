/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.bitlap.core.data.metadata.Table

/**
 * Hadoop Compatible File System common utils
 */
object Hcfs {

    /**
     * Write bitlap table schema.
     */
    fun FileSystem.writeTable(tableDir: Path, table: Table): Boolean {
        val tablePB = table.buildPB().toByteArray()
        this.create(Path(tableDir, ".table"), true).use {
            it.writeInt(tablePB.size)
            it.write(tablePB)
        }
        return true
    }

    /**
     * Read bitlap table schema.
     */
    fun FileSystem.readTable(tableDir: Path): Table {
        return this.open(Path(tableDir, ".table")).use {
            val len = it.readInt()
            val buf = ByteArray(len)
            it.readFully(buf, 0, len)
            Table.from(buf, tableDir.toString())
        }
    }

    /**
     * clone conf
     */
    fun FileSystem.newConf(): Configuration {
        return Configuration(this.conf)
    }
}

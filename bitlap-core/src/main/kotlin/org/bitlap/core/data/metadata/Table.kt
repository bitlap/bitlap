/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.data.metadata

import org.bitlap.common.schema.TablePB
import org.bitlap.core.storage.StoreType

/**
 * Table
 */
data class Table(
    private val _database: String,
    private val _name: String,
    val createTime: Long = System.currentTimeMillis(),
    var updateTime: Long = System.currentTimeMillis(),
    val props: Map<String, String> = mutableMapOf(),
    // other fields
    val path: String,
) {

    val database: String
        get() = _database.lowercase()
    val name: String
        get() = _name.lowercase()

    fun buildPB(): TablePB = TablePB.newBuilder()
        .setDatabase(database)
        .setName(name)
        .setCreateTime(createTime)
        .setUpdateTime(updateTime)
        .putAllProps(props)
        .build()

    override fun toString(): String {
        return "$database.$name"
    }

    fun getTableFormat(): StoreType = StoreType.valueOf(this.props[TABLE_FORMAT_KEY]!!)

    companion object {

        fun from(bytes: ByteArray, path: String): Table {
            return TablePB.parseFrom(bytes).run {
                Table(database, name, createTime, updateTime, propsMap, path)
            }
        }

        // table properties
        const val TABLE_FORMAT_KEY = "table_format"
    }
}

/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.catalog.metadata

/**
 * Table metadata
 */
data class Table(
    val database: String,
    val name: String,
    val createTime: Long = System.currentTimeMillis(),
    var updateTime: Long = System.currentTimeMillis(),
    val props: Map<String, String> = mutableMapOf(),
    // other fields
    val path: String,
) {

    override fun toString(): String {
        return "$database.$name"
    }

    fun getTableFormat(): String = this.props[TABLE_FORMAT_KEY]!!

    companion object {
        // table properties
        const val TABLE_FORMAT_KEY = "table_format"
    }
}

package org.bitlap.core.data.metadata

import org.bitlap.network.proto.metadata.TablePB

/**
 * Table
 */
data class Table(
    private val _database: String,
    private val _name: String,
    val createTime: Long = System.currentTimeMillis(),
    var updateTime: Long = System.currentTimeMillis(),
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
        .build()

    companion object {

        fun from(bytes: ByteArray): Table {
            return TablePB.parseFrom(bytes).run {
                Table(database, name, createTime, updateTime)
            }
        }
    }
}

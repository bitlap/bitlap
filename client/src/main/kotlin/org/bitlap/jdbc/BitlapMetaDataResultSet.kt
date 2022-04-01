/* Copyright (c) 2022 bitlap.org */
package org.bitlap.jdbc

/**
 *
 * @author 梦境迷离
 * @since 2021/8/23
 * @version 1.0
 */
abstract class BitlapMetaDataResultSet<M>(
    override var columnNames: MutableList<String> = ArrayList(),
    override var columnTypes: MutableList<String> = ArrayList(),
    protected var data: List<M> = mutableListOf()
) : BitlapBaseResultSet() {

    override fun close() {
    }
}

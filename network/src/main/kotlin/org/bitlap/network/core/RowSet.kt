package org.bitlap.network.core

import com.google.protobuf.ByteString
import org.bitlap.network.proto.driver.BRow
import org.bitlap.network.proto.driver.BRowSet

/**
 * The wrapper class of the Proto buffer `BRowSet`.
 *
 * @author 梦境迷离
 * @since 2021/8/27
 * @version 1.0
 */
class RowSet(val rows: List<Row> = listOf(), val startOffset: Long = 0) {

    fun toBRowSet(): BRowSet {
        return BRowSet.newBuilder().setStartRowOffset(startOffset).addAllRows(rows.map { it.toBRow() }).build()
    }
}

/**
 * The wrapper class of the Proto buffer `BRow`.
 */
class Row(private val values: List<ByteString> = listOf()) {

    fun toBRow(): BRow {
        return BRow.newBuilder().addAllColVals(values).build()
    }
}

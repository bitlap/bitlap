package org.bitlap.network.core

import com.google.protobuf.ByteString
import org.bitlap.network.proto.driver.BRow
import org.bitlap.network.proto.driver.BRowSet

/**
 * Wrapper for protoc buffer
 *
 * @author 梦境迷离
 * @since 2021/8/27
 * @version 1.0
 */
class RowSet(private val rows: List<Row> = listOf(), private val startOffset: Long = 0) {

    fun toBRowSet(): BRowSet {
        return BRowSet.newBuilder().setStartRowOffset(startOffset).addAllRows(rows.map { it.toBRow() }).build()
    }
}

class Row(private val values: List<ByteString> = listOf()) {

    fun toBRow(): BRow {
        return BRow.newBuilder().addAllColVals(values).build()
    }
}

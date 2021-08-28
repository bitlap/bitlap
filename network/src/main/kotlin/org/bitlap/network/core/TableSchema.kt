package org.bitlap.network.core

import org.bitlap.network.proto.driver.BColumnDesc
import org.bitlap.network.proto.driver.BTableSchema
import org.bitlap.network.proto.driver.BTypeId

/**
 * Wrapper for protoc buffer
 * @author 梦境迷离
 * @since 2021/8/28
 * @version 1.0
 */
class TableSchema(private var columns: List<ColumnDesc> = listOf()) {
    fun toBTableSchema(): BTableSchema {
        return BTableSchema.newBuilder().addAllColumns(columns.map { it.toBColumnDesc() }).build()
    }
}

class ColumnDesc(
    private val columnName: String,
    private val typeDesc: TypeId
) {
    fun toBColumnDesc(): BColumnDesc {
        return BColumnDesc.newBuilder().setTypeDesc(typeDesc.bTypeId).setColumnName(columnName).build()
    }
}

enum class TypeId(val bTypeId: BTypeId) {
    B_TYPE_ID_UNSPECIFIED(BTypeId.B_TYPE_ID_UNSPECIFIED),
    B_TYPE_ID_STRING_TYPE(BTypeId.B_TYPE_ID_STRING_TYPE),
    B_TYPE_ID_INT_TYPE(BTypeId.B_TYPE_ID_INT_TYPE),
    B_TYPE_ID_DOUBLE_TYPE(BTypeId.B_TYPE_ID_DOUBLE_TYPE);

    fun toBOperationType(): BTypeId {
        return this.bTypeId
    }
}

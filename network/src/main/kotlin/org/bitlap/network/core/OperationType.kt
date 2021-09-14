package org.bitlap.network.core

import org.bitlap.network.proto.driver.BOperationType

/**
 * The wrapper class of the Proto buffer `BOperationType`.
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
enum class OperationType(val bOpType: BOperationType) {
    EXECUTE_STATEMENT(BOperationType.B_OPERATION_TYPE_EXECUTE_STATEMENT),
    GET_COLUMNS(BOperationType.B_OPERATION_TYPE_GET_COLUMNS),
    GET_SCHEMAS(BOperationType.B_OPERATION_TYPE_GET_SCHEMAS),
    GET_TABLES(BOperationType.B_OPERATION_TYPE_GET_TABLES),
    UNKNOWN_OPERATION(BOperationType.UNRECOGNIZED);

    companion object {
        fun getOperationType(bOperationType: BOperationType): OperationType {
            return values().find { it.bOpType == bOperationType } ?: UNKNOWN_OPERATION
        }
    }

    fun toBOperationType(): BOperationType {
        return this.bOpType
    }
}

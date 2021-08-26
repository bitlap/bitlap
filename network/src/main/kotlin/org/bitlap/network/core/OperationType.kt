package org.bitlap.network.core

import org.bitlap.network.proto.driver.BOperationType

/**
 * Operation Type
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
enum class OperationType(bOpType: BOperationType) {
    // TODO others
    EXECUTE_STATEMENT(BOperationType.B_OPERATION_TYPE_EXECUTE_STATEMENT),
    UNKNOWN_OPERATION(BOperationType.UNRECOGNIZED);

    private var bOperationType: BOperationType = bOpType

    companion object {
        fun getOperationType(bOperationType: BOperationType): OperationType {
            return values().find { it.bOperationType == bOperationType } ?: UNKNOWN_OPERATION
        }
    }

    open fun toBOperationType(): BOperationType {
        return bOperationType
    }
}

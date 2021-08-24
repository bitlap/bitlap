package org.bitlap.server.raft.cli

import org.bitlap.common.proto.driver.BOperationHandle

/**
 * Operation Handle
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
open class OperationHandle : Handle {

    private var opType: OperationType
    private var hasResultSet = false

    constructor(opType: OperationType) : super() {
        this.opType = opType
    }

    constructor(bOperationHandle: BOperationHandle) : super(bOperationHandle.operationId) {
        this.opType = OperationType.getOperationType(bOperationHandle.operationType)
        this.hasResultSet = bOperationHandle.hasResultSet
    }

    fun toBOperationHandle(): BOperationHandle {
        return BOperationHandle.newBuilder().setHasResultSet(hasResultSet)
            .setOperationId(getHandleIdentifier().toBHandleIdentifier())
            .setOperationType(opType.toBOperationType()).build()
    }

    override fun hashCode(): Int {
        val prime = 31
        var result = super.hashCode()
        result = prime * result + opType.hashCode()
        return result
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (!super.equals(other)) {
            return false
        }
        if (other !is OperationHandle) {
            return false
        }
        return opType === other.opType
    }

    override fun toString(): String {
        return "OperationHandle [opType=" + opType + ", getHandleIdentifier()=" + getHandleIdentifier() + "]"
    }
}

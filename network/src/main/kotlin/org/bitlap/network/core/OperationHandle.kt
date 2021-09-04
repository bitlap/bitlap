package org.bitlap.network.core

import org.bitlap.network.proto.driver.BOperationHandle
import org.bitlap.network.proto.driver.BSessionHandle

/**
 * Operation Handle
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
open class OperationHandle(
    val sessionHandle: SessionHandle,
    override val handleId: HandleIdentifier,
    private val opType: OperationType,
    private val hasResultSet: Boolean = false
) : Handle(handleId) {

    constructor(bSessionHandle: BSessionHandle, bOperationHandle: BOperationHandle) : this(
        SessionHandle(bSessionHandle),
        HandleIdentifier(bOperationHandle.operationId),
        OperationType.getOperationType(bOperationHandle.operationType),
        bOperationHandle.hasResultSet
    )

    fun toBOperationHandle(sessionHandle: BSessionHandle): BOperationHandle {
        return BOperationHandle.newBuilder().setHasResultSet(hasResultSet)
            .setSessionHandle(sessionHandle)
            .setOperationId(super.handleId.toBHandleIdentifier())
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
        return "OperationHandle [opType=" + opType + ", handleId()=" + super.handleId + "]"
    }
}

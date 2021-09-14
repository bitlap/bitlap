package org.bitlap.network.core.operation

import org.bitlap.network.core.Handle
import org.bitlap.network.core.HandleIdentifier
import org.bitlap.network.core.OperationType
import org.bitlap.network.proto.driver.BOperationHandle

/**
 * The wrapper class of the Proto buffer `BOperationHandle`.
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
open class OperationHandle(
    private val opType: OperationType,
    private val hasResultSet: Boolean = false,
    override val handleId: HandleIdentifier = HandleIdentifier(),
) : Handle(handleId) {

    constructor(bOperationHandle: BOperationHandle) : this(
        OperationType.getOperationType(bOperationHandle.operationType),
        bOperationHandle.hasResultSet,
        HandleIdentifier(bOperationHandle.operationId),
    )

    fun toBOperationHandle(): BOperationHandle {
        return BOperationHandle.newBuilder().setHasResultSet(hasResultSet)
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

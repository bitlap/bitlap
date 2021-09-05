package org.bitlap.network.core.operation

import org.bitlap.network.BSQLException
import org.bitlap.network.core.Session

/**
 *
 * @author 梦境迷离
 * @since 2021/9/5
 * @version 1.0
 */
class OperationManager {

    private val handleToOperation: MutableMap<OperationHandle, Operation> = mutableMapOf()

    fun newExecuteStatementOperation(
        parentSession: Session,
        statement: String,
        confOverlay: Map<String, String>?
    ): Operation {
        val executeStatementOperation = Operation
            .newExecuteStatementOperation(parentSession, statement, confOverlay)
        addOperation(executeStatementOperation)
        return executeStatementOperation
    }

    @Synchronized
    fun addOperation(operation: Operation) {
        handleToOperation[operation.opHandle] = operation
    }

    @Synchronized
    fun getOperation(operationHandle: OperationHandle): Operation {
        return handleToOperation[operationHandle]
            ?: throw BSQLException("Invalid OperationHandle: $operationHandle")
    }
}

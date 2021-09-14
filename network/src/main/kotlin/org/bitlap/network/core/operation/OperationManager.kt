package org.bitlap.network.core.operation

import cn.hutool.core.util.ServiceLoaderUtil
import org.bitlap.network.BSQLException
import org.bitlap.network.core.OperationType
import org.bitlap.network.core.Session

/**
 * Lifecycle management of operations.
 *
 * @author 梦境迷离
 * @since 2021/9/5
 * @version 1.0
 */
class OperationManager {

    private val operationFactory = ServiceLoaderUtil.loadFirst(OperationFactory::class.java)!!
    private val handleToOperation: MutableMap<OperationHandle, Operation> = mutableMapOf()

    /**
     * Create an operation for the SQL and execute it. For now, we put the results in memory by Map.
     */
    fun newExecuteStatementOperation(
        parentSession: Session,
        statement: String,
        confOverlay: Map<String, String>?
    ): Operation {
        val operation = operationFactory.create(parentSession, OperationType.EXECUTE_STATEMENT, true)
        operation.confOverlay = confOverlay
        operation.statement = statement
        operation.run()
        addOperation(operation)
        return operation
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

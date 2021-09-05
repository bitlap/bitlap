package org.bitlap.network.core.operation

import org.bitlap.common.logger
import org.bitlap.network.core.OperationType
import org.bitlap.network.core.Session

/**
 * This class should abstract according to different operations, and each operation is a special implementation.
 *
 * @author 梦境迷离
 * @since 2021/9/5
 * @version 1.0
 */
open class Operation(val parentSession: Session, val opType: OperationType, val hasResultSet: Boolean = false) {
    protected val log = logger { }

    val opHandle: OperationHandle by lazy { OperationHandle(opType, hasResultSet) }

    protected var statement: String? = null
    protected var confOverlay: Map<String, String>? = mutableMapOf()

    companion object {
        @JvmStatic
        fun newExecuteStatementOperation(
            parentSession: Session,
            statement: String,
            confOverlay: Map<String, String>?
        ): Operation {
            val operationHandle = Operation(parentSession, OperationType.EXECUTE_STATEMENT, false)
            operationHandle.confOverlay = confOverlay
            operationHandle.statement = statement
            return operationHandle
        }
    }
}

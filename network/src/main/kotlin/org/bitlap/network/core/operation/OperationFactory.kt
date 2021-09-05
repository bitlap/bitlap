package org.bitlap.network.core.operation

import org.bitlap.network.core.OperationType
import org.bitlap.network.core.Session

interface OperationFactory {

    fun create(parentSession: Session, opType: OperationType, hasResultSet: Boolean = false): Operation
}

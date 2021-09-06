package org.bitlap.server.core

import org.bitlap.network.core.OperationType
import org.bitlap.network.core.Session
import org.bitlap.network.core.operation.Operation
import org.bitlap.network.core.operation.OperationFactory

class BitlapOperationFactory : OperationFactory {
    override fun create(parentSession: Session, opType: OperationType, hasResultSet: Boolean): Operation {
        return BitlapOperation(parentSession, opType, hasResultSet)
    }
}

package org.bitlap.server.core

import org.bitlap.net.operation.OperationFactory
import org.bitlap.net.operation.OperationType
import org.bitlap.net.operation.operations
import org.bitlap.net.session.Session

class BitlapOperationFactory : OperationFactory {
    override fun create(parentSession: Session, opType: OperationType, hasResultSet: Boolean): operations.Operation {
        return BitlapOperation(parentSession, opType, hasResultSet)
    }
}

package org.bitlap.server.core

import org.bitlap.core.sql.QueryExecution
import org.bitlap.network.core.OperationType
import org.bitlap.network.core.Session
import org.bitlap.network.core.operation.Operation

/**
 *
 * @author 梦境迷离
 * @since 2021/9/5
 * @version 1.0
 */
class BitlapOperation(parentSession: Session, opType: OperationType, hasResultSet: Boolean = false) :
    Operation(parentSession, opType, hasResultSet) {

    override fun run() {
        cache[super.opHandle] = QueryExecution(super.statement).execute()
    }
}

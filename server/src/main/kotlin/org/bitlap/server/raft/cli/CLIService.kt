package org.bitlap.server.raft.cli

import org.bitlap.common.proto.driver.BRowSet
import org.bitlap.common.proto.driver.BTableSchema


/**
 * Interface definition of driver RPC.
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
interface CLIService {

    fun openSession(username: String, password: String, configuration: Map<String, String>?): SessionHandle

    fun closeSession(sessionHandle: SessionHandle)

    fun executeStatement(
        sessionHandle: SessionHandle,
        statement: String,
        confOverlay: Map<String, String>?
    ): OperationHandle

    fun executeStatement(
        sessionHandle: SessionHandle,
        statement: String,
        confOverlay: Map<String, String>?,
        queryTimeout: Long
    ): OperationHandle

    fun fetchResults(opHandle: OperationHandle): BRowSet

    fun getResultSetMetadata(
        opHandle: OperationHandle,
    ): BTableSchema //convert BTableSchema to kotlin class

    // other methods
}

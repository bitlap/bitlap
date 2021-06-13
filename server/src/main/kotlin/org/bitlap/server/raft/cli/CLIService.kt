package org.bitlap.server.raft.cli

/**
 * Interface definition of driver RPC.
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
interface CLIService {

    @Throws(BSQLException::class)
    fun openSession(username: String, password: String, configuration: Map<String, String>?): SessionHandle?

    @Throws(BSQLException::class)
    fun closeSession(sessionHandle: SessionHandle)

    @Throws(BSQLException::class)
    fun executeStatement(
        sessionHandle: SessionHandle,
        statement: String,
        confOverlay: Map<String, String>?
    ): OperationHandle?

    @Throws(BSQLException::class)
    fun executeStatement(
        sessionHandle: SessionHandle,
        statement: String,
        confOverlay: Map<String, String>?,
        queryTimeout: Long
    ): OperationHandle?

    @Throws(BSQLException::class)
    fun fetchResults(opHandle: OperationHandle): List<String?>? // TODO use RowSet

    // other methods
}

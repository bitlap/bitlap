package org.bitlap.server.raft.cli

/**
 * Implementation of driver RPC core.
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
open class BCLIService(private val sessionManager: SessionManager) : CLIService {

    // get session manager by conf
    // create session
    // execute statement by session
    // return OperationHandle
    // fetch results by OperationHandle

    override fun openSession(
        username: String,
        password: String,
        configuration: Map<String, String>?
    ): SessionHandle? {
        return sessionManager.openSession(
            null, username, password,  configuration ?: mapOf()
        ).sessionHandle
    }

    override fun closeSession(sessionHandle: SessionHandle) {
        sessionManager.closeSession(sessionHandle)
    }

    override fun executeStatement(
        sessionHandle: SessionHandle,
        statement: String,
        confOverlay: Map<String, String>?
    ): OperationHandle? {
        TODO("Not yet implemented")
    }

    override fun executeStatement(
        sessionHandle: SessionHandle,
        statement: String,
        confOverlay: Map<String, String>?,
        queryTimeout: Long
    ): OperationHandle? {
        TODO("Not yet implemented")
    }

    override fun fetchResults(opHandle: OperationHandle): List<String?>? {
        TODO("Not yet implemented")
    }
}
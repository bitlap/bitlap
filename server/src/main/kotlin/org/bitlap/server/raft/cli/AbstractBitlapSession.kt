package org.bitlap.server.raft.cli

/**
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
interface AbstractBitlapSession {

    /**
     * Set the session manager for the session
     * @param sessionManager
     */
    fun setSessionManager(sessionManager: SessionManager)
    /**
     * Get the session manager for the session
     */
    fun getSessionManager(): SessionManager

    @Throws(Exception::class)
    fun open(sessionConfMap: Map<String, String>?)

    /**
     * execute operation handler
     * @param statement
     * @param confOverlay
     * @return
     * @throws BSQLException
     */
    @Throws(BSQLException::class)
    fun executeStatement(
        statement: String,
        confOverlay: Map<String, String>?
    ): OperationHandle?

    /**
     * execute operation handler
     * @param statement
     * @param confOverlay
     * @param queryTimeout
     * @return
     * @throws BSQLException
     */
    @Throws(BSQLException::class)
    fun executeStatement(
        statement: String,
        confOverlay: Map<String, String>?,
        queryTimeout: Long
    ): OperationHandle?

    @Throws(BSQLException::class)
    fun close()
}

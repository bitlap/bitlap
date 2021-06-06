package org.bitlap.server.raft.cli

/**
 * Bitlap Session
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
open class BitlapSession : AbstractBitlapSession {

    private lateinit var sessionManager: SessionManager

    override fun setSessionManager(sessionManager: SessionManager) {
        this.sessionManager = sessionManager
    }

    override fun getSessionManager(): SessionManager = this.sessionManager

    override fun open(sessionConfMap: Map<String, String>?) {
        TODO("Not yet implemented")
    }

    override fun executeStatement(statement: String, confOverlay: Map<String, String>?): OperationHandle? {
        TODO("Not yet implemented")
    }

    override fun executeStatement(
        statement: String,
        confOverlay: Map<String, String>?,
        queryTimeout: Long
    ): OperationHandle? {
        TODO("Not yet implemented")
    }

    override fun close() {
        TODO("Not yet implemented")
    }
}

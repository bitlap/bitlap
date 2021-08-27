package org.bitlap.network.core

import java.util.concurrent.atomic.AtomicBoolean
import org.bitlap.common.BitlapConf

/**
 * Bitlap Session
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
class BitlapSession() : Session {

    @Volatile
    override var lastAccessTime: Long = System.currentTimeMillis()
    override lateinit var username: String
    override lateinit var password: String
    override lateinit var sessionHandle: SessionHandle
    override lateinit var sessionConf: BitlapConf
    override val creationTime: Long = System.currentTimeMillis()
    override lateinit var sessionManager: SessionManager // TODO add operationManager
    override val sessionState: AtomicBoolean = AtomicBoolean(false)

    constructor(
        username: String,
        password: String,
        sessionConf: Map<String, String>,
        sessionManager: SessionManager,
        sessionHandle: SessionHandle = SessionHandle(HandleIdentifier())
    ) : this() {
        this.username = username
        this.sessionHandle = sessionHandle
        this.password = password
        this.sessionConf = BitlapConf(sessionConf)
        this.sessionState.compareAndSet(false, true)
        this.sessionManager = sessionManager
    }

    override fun open(sessionConfMap: Map<String, String>?): SessionHandle {
        TODO("Not yet implemented")
    }

    override fun executeStatement(statement: String, confOverlay: Map<String, String>?): OperationHandle {
        return OperationHandle(OperationType.EXECUTE_STATEMENT, true)
    }

    override fun executeStatement(
        statement: String,
        confOverlay: Map<String, String>?,
        queryTimeout: Long
    ): OperationHandle {
        return executeStatement(statement, confOverlay)
    }

    override fun close() {
        TODO("Not yet implemented")
    }
}

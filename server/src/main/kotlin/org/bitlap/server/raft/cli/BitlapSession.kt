package org.bitlap.server.raft.cli

import org.bitlap.common.BitlapConf
import java.util.concurrent.atomic.AtomicBoolean


/**
 * Bitlap Session
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
class BitlapSession : AbstractBitlapSession {

    @Volatile
    override var lastAccessTime: Long = 0
    override lateinit var username: String
    override lateinit var password: String
    override lateinit var sessionHandle: SessionHandle
    override lateinit var sessionConf: BitlapConf
    override var creationTime: Long = 0
    override lateinit var sessionManager: SessionManager
    override val sessionState: AtomicBoolean = AtomicBoolean(false)

    constructor()
    constructor(
        sessionHandle: SessionHandle?,
        username: String,
        password: String,
        sessionConf: Map<String, String>,
        sessionManager: SessionManager
    ) : this() {
        this.username = username
        this.sessionHandle = sessionHandle ?: SessionHandle()
        this.password = password
        this.sessionConf = BitlapConf(sessionConf)
        this.creationTime = System.currentTimeMillis()
        this.lastAccessTime = System.currentTimeMillis()
        this.sessionState.compareAndSet(false, true)
        this.sessionManager = sessionManager
    }

    override fun open(sessionConfMap: Map<String, String>?): SessionHandle {
        TODO("Not yet implemented")
    }

    override fun executeStatement(statement: String, confOverlay: Map<String, String>?): OperationHandle {
        TODO("Not yet implemented")
    }

    override fun executeStatement(
        statement: String,
        confOverlay: Map<String, String>?,
        queryTimeout: Long
    ): OperationHandle {
        TODO("Not yet implemented")
    }

    override fun close() {
        TODO("Not yet implemented")
    }
}

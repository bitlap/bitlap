package org.bitlap.server.raft.cli

import java.util.concurrent.atomic.AtomicBoolean
import org.bitlap.common.BitlapConf
import org.bitlap.common.exception.BitlapException

/**
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
interface AbstractBSession {

    val sessionState: AtomicBoolean
    var lastAccessTime: Long
    val sessionHandle: SessionHandle
    val password: String
    val username: String
    val creationTime: Long
    val sessionConf: BitlapConf
    val sessionManager: SessionManager

    /**
     * open Session
     * @param sessionConfMap
     * @return SessionHandle The Session handle
     */
    @Throws(BitlapException::class)
    fun open(sessionConfMap: Map<String, String>?): SessionHandle

    /**
     * execute statement
     * @param statement
     * @param confOverlay
     * @return OperationHandle The Operate handle
     */
    @Throws(BitlapException::class)
    fun executeStatement(
        statement: String,
        confOverlay: Map<String, String>?
    ): OperationHandle

    /**
     * execute statement
     * @param statement
     * @param confOverlay
     * @param queryTimeout
     * @return OperationHandle The Operate handle
     */
    fun executeStatement(
        statement: String,
        confOverlay: Map<String, String>?,
        queryTimeout: Long
    ): OperationHandle

    /**
     * close Session
     */
    fun close()
}

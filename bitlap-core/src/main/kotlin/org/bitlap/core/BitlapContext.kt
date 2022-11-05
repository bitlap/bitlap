/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core

import org.apache.hadoop.conf.Configuration
import org.bitlap.common.BitlapConf
import org.bitlap.common.EventBus
import org.bitlap.core.data.impl.BitlapCatalogImpl
import org.bitlap.core.sql.BitlapSqlPlanner
import org.bitlap.network.handles.HandleIdentifier
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Desc: Context with core components.
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/5/30
 */
object BitlapContext {

    val bitlapConf = BitlapConf()
    val hadoopConf = Configuration()

    val catalog by lazy {
        BitlapCatalogImpl(bitlapConf, hadoopConf).apply {
            start()
        }
    }

    val sqlPlanner by lazy {
        BitlapSqlPlanner(catalog)
    }

    val eventBus by lazy {
        EventBus().apply { start() }
    }

    private val sessionMap = mutableMapOf<HandleIdentifier, SessionContext>()

    fun getSession(sessionId: SessionId): SessionContext {
        return sessionMap.getOrPut(sessionId.id) { SessionContext.fakeSession(sessionId) }
    }

    fun updateSession(sessionContext: SessionContext) {
        sessionMap[sessionContext.sessionId.id] = sessionContext
    }

    fun putIfAbsentSession(sessionContext: SessionContext) {
        sessionMap.putIfAbsent(sessionContext.sessionId.id, sessionContext)
    }
}

data class SessionId(
    val id: HandleIdentifier
) {
    companion object {
        fun fakeSessionId(): SessionId {
            return SessionId(HandleIdentifier(UUID.randomUUID(), UUID.randomUUID()))
        }
    }
}

data class SessionContext(
    val sessionId: SessionId,
    val status: AtomicBoolean,
    val createTime: Long,
    val currentSchema: String,
) {
    companion object {
        fun fakeSession(): SessionContext {
            return SessionContext(
                SessionId.fakeSessionId(), AtomicBoolean(true), System.currentTimeMillis(), Constants.DEFAULT_DATABASE
            )
        }

        fun fakeSession(sessionId: SessionId): SessionContext {
            return SessionContext(
                sessionId, AtomicBoolean(true), System.currentTimeMillis(), Constants.DEFAULT_DATABASE
            )
        }
    }
}

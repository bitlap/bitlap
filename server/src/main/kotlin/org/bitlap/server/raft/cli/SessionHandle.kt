package org.bitlap.server.raft.cli

import org.bitlap.common.proto.driver.BSessionHandle
import java.util.UUID

/**
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
open class SessionHandle : Handle {

    constructor()
    constructor(bSessionHandle: BSessionHandle) : super(bSessionHandle.sessionId)
    constructor(handleId: HandleIdentifier) : super(handleId)

    fun toBSessionHandle(): BSessionHandle {
        return BSessionHandle.newBuilder().setSessionId(super.getHandleIdentifier().toBHandleIdentifier()).build()
    }

    fun getSessionId(): UUID {
        return getHandleIdentifier().getPublicId()
    }

    override fun toString(): String = "SessionHandle [" + super.getHandleIdentifier().toString() + "]"
}

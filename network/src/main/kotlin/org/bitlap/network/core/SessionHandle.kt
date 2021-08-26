package org.bitlap.network.core

import org.bitlap.network.proto.driver.BSessionHandle
import java.util.UUID

/**
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
open class SessionHandle(private val handleId: HandleIdentifier) : Handle(handleId) {

    constructor(bSessionHandle: BSessionHandle) : this(HandleIdentifier(bSessionHandle.sessionId))

    fun toBSessionHandle(): BSessionHandle {
        return BSessionHandle.newBuilder().setSessionId(super.getHandleIdentifier().toBHandleIdentifier()).build()
    }

    fun getSessionId(): UUID {
        return handleId.publicId
    }

    override fun toString(): String = "SessionHandle [" + super.getHandleIdentifier().toString() + "]"
}

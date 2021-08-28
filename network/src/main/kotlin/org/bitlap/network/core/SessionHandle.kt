package org.bitlap.network.core

import org.bitlap.network.proto.driver.BSessionHandle

/**
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
open class SessionHandle(override val handleId: HandleIdentifier) : Handle(handleId) {

    constructor(bSessionHandle: BSessionHandle) : this(HandleIdentifier(bSessionHandle.sessionId))

    fun toBSessionHandle(): BSessionHandle {
        return BSessionHandle.newBuilder().setSessionId(super.handleId.toBHandleIdentifier()).build()
    }

    override fun toString(): String = "SessionHandle [$handleId]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SessionHandle) return false
        if (!super.equals(other)) return false

        if (handleId != other.handleId) return false

        return true
    }

    override fun hashCode(): Int {
        var result = super.hashCode()
        result = 31 * result + handleId.hashCode()
        return result
    }
}

package org.bitlap.server.raft.cli

import org.bitlap.common.BitlapConf

/**
 *
 * BSessionHookContextImpl.
 * Session hook context implementation which is created by session  manager
 * and passed to hook invocation.
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
class BSessionHookContextImpl(private val abstractBSession: AbstractBSession) : BSessionHookContext {

    override fun getSessionConf(): BitlapConf {
        return abstractBSession.sessionConf
    }

    override fun getSessionUser(): String {
        return abstractBSession.username
    }

    override fun getSessionHandle(): String {
        return abstractBSession.sessionHandle.toString()
    }
}

package org.bitlap.server.raft.cli

import java.util.concurrent.ConcurrentHashMap

/**
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
open class SessionManager {

    private val handleToSession = ConcurrentHashMap<SessionHandle, AbstractBitlapSession>()

    // service, provider, conf, discover
    // session life cycle manage
}

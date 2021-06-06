package org.bitlap.server.raft.cli

import org.bitlap.common.BitlapConf

/**
 * BSessionHookContext.
 * Interface passed to the HiveServer2 session hook execution. This enables
 * the hook implementation to accesss session config, user and session handle
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
interface BSessionHookContext {

    /**
     * Retrieve session conf
     * @return
     */
    fun getSessionConf(): BitlapConf?

    /**
     * The get the username starting the session
     * @return
     */
    fun getSessionUser(): String?

    /**
     * Retrieve handle for the session
     * @return
     */
    fun getSessionHandle(): String?
}

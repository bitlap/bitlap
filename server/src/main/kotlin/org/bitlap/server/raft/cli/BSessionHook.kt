package org.bitlap.server.raft.cli

/**
 * BSessionHook.
 * BitlapServer session level Hook interface. The run method is executed
 *  when session manager starts a new session
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
interface BSessionHook : Hook {

    /**
     * @param sessionHookContext context
     * @throws HiveSQLException
     */
    @Throws(BSQLException::class)
    fun run(sessionHookContext: BSessionHookContext)

}

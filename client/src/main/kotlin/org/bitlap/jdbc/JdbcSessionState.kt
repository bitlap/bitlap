package org.bitlap.jdbc

import org.bitlap.common.BitlapConf

/**
 *
 * @author 梦境迷离
 * @since 2021/6/12
 * @version 1.0
 */
class JdbcSessionState(private val conf: BitlapConf?) { // current configuration

    companion object {

        /**
         * Singleton Session object per thread.
         *
         */
        private val tss: ThreadLocal<JdbcSessionState> = ThreadLocal<JdbcSessionState>()

        /**
         * set current session to existing session object
         * if a thread is running multiple sessions - it must call this method with the new
         * session object when switching from one session to another
         */
        fun start(startSs: JdbcSessionState): JdbcSessionState {
            tss.set(startSs)
            return startSs
        }

        /**
         * get the current session
         */
        fun get(): JdbcSessionState {
            return tss.get()
        }
    }
}

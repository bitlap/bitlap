package org.bitlap.server.raft.cli

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

/**
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
open class SessionManager {

    private val handleToSession = ConcurrentHashMap<SessionHandle, AbstractBSession>()
    private val sessionAddLock = Any()
    private val sessionThread = Thread {// register center
        while (true) {
            val iterator = handleToSession.iterator()
            println("There are [${handleToSession.size}] surviving sessions")
            try {
                while (iterator.hasNext()) {
                    val element = iterator.next()
                    val sessionHandle = element.key
                    if (!element.value.sessionState.get()) {
                        iterator.remove()
                        println("Session state is false, remove session: $sessionHandle")
                    }

                    val now = System.currentTimeMillis()
                    if (element.value.lastAccessTime + 20 * 60 * 1000 < now) {
                        iterator.remove()
                        println("Session has not been visited for 20 minutes, remove session: $sessionHandle")
                    }
                }

                TimeUnit.SECONDS.sleep(1)

            } catch (e: Exception) {
                println("Failed to listen for session error: $e.localizedMessage")
            }
        }
    }

    init {
        sessionThread.start()
    }

    // service, provider, conf, discover
    // session life cycle manage

    @Throws(BSQLException::class)
    fun openSession(
        sessionHandle: SessionHandle?,
        username: String,
        password: String,
        sessionConf: Map<String, String>
    ): BSession {
        synchronized(sessionAddLock) {
            val session = BSession(
                sessionHandle,
                username,
                password,
                sessionConf,
                this
            )

            try {
                executeSessionHooks(session)
            } catch (e: Exception) {
                println("Failed to execute session hooks: $e")
                try {
                    session.close()
                } catch (t: Throwable) {
                    println("Error closing session: $t")
                }
                throw BSQLException("Failed to execute session hooks: " + e.message, e)
            }

            handleToSession[session.sessionHandle] = session
            println(
                "Session opened, " + session.sessionHandle.toString() + ", current sessions:" + getOpenSessionCount()
            )

            return session
        }
    }

    @Throws(BSQLException::class)
    fun closeSession(sessionHandle: SessionHandle) {
        synchronized(sessionAddLock) {
            handleToSession.remove(sessionHandle) ?: throw BSQLException("Session does not exist: $sessionHandle")
            println("Session closed, " + sessionHandle + ", current sessions:" + getOpenSessionCount())
            if (getOpenSessionCount() == 0) {
                println(
                    "This instance of Bitlap has been removed from the list of server "
                        + "instances available for dynamic service discovery. "
                        + "The last client session has ended - will shutdown now."
                )
                // TODO STOP server
            }
        }
    }

    open fun getOpenSessionCount(): Int {
        return handleToSession.size
    }

    private fun executeSessionHooks(abstractBSession: AbstractBSession) {
        //TODO need HookContext, and get read hooks from conf
        val sessionHooks: List<BSessionHook> = listOf()
        for (sessionHook in sessionHooks) {
            sessionHook.run(BSessionHookContextImpl(abstractBSession))
        }
    }
}

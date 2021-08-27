package org.bitlap.network.core

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import org.bitlap.common.exception.BitlapException

/**
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
class SessionManager {

    private val handleToSession = ConcurrentHashMap<SessionHandle, Session>()
    private val sessionAddLock = Any()
    private val sessionThread = Thread { // register center
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
                    } else {
                        println("SessionId: ${sessionHandle.handleId}")
                    }
                }

                TimeUnit.SECONDS.sleep(3)
            } catch (e: Exception) {
                println("Failed to listen for session error: $e.localizedMessage")
            }
        }
    }

    init {
        sessionThread.isDaemon = true
        sessionThread.start()
    }

    // service, provider, conf, discover
    // session life cycle manage

    fun openSession(
        username: String,
        password: String,
        sessionConf: Map<String, String>
    ): BitlapSession {

        println("Server get properties [username:$username, password:$password, sessionConf:$sessionConf]")
        synchronized(sessionAddLock) {
            val session = BitlapSession(
                username,
                password,
                sessionConf,
                this
            )
            handleToSession[session.sessionHandle] = session
            println("Create session: ${session.sessionHandle}")
            return session
        }
    }

    fun closeSession(sessionHandle: SessionHandle) {
        synchronized(sessionAddLock) {
            handleToSession.remove(sessionHandle) ?: throw BitlapException("Session does not exist: $sessionHandle")
            println("Session closed, " + sessionHandle + ", current sessions:" + getOpenSessionCount())
            if (getOpenSessionCount() == 0) {
                println(
                    "This instance of Bitlap has been removed from the list of server " +
                        "instances available for dynamic service discovery. " +
                        "The last client session has ended - will shutdown now."
                )
                // TODO STOP server
            }
        }
    }

    private fun getOpenSessionCount(): Int {
        return handleToSession.size
    }

    fun getSession(sessionHandle: SessionHandle): Session {
        var session: Session?
        synchronized(sessionAddLock) {
            session = handleToSession[sessionHandle]
        }
        if (session == null) {
            throw BitlapException("Invalid SessionHandle: $sessionHandle")
        }
        return session!!
    }

    fun refreshSession(sessionHandle: SessionHandle, session: Session) {
        synchronized(sessionAddLock) {
            session.lastAccessTime = System.currentTimeMillis()
            if (handleToSession.containsKey(sessionHandle)) {
                handleToSession[sessionHandle] = session
            } else {
                throw BitlapException("Invalid SessionHandle: $sessionHandle")
            }
        }
    }
}

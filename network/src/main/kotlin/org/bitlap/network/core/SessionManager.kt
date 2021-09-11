package org.bitlap.network.core

import cn.hutool.core.util.ServiceLoaderUtil
import org.bitlap.common.exception.BitlapException
import org.bitlap.common.logger
import org.bitlap.network.core.operation.OperationManager
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

/**
 * Lifecycle management of sessions.
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
class SessionManager {

    val operationManager by lazy { OperationManager() }
    private val sessionFactory = ServiceLoaderUtil.loadFirst(SessionFactory::class.java)!!

    private val log = logger { }
    private val handleToSession by lazy { ConcurrentHashMap<SessionHandle, Session>() }
    private val sessionAddLock by lazy { Any() }
    private val sessionThread = Thread { // register center
        while (true) {
            val iterator = handleToSession.iterator()
            log.info("There are [${handleToSession.size}] surviving sessions")
            try {
                while (iterator.hasNext()) {
                    val element = iterator.next()
                    val sessionHandle = element.key
                    if (!element.value.sessionState.get()) {
                        iterator.remove()
                        log.info("Session state is false, remove session: $sessionHandle")
                    }

                    val now = System.currentTimeMillis()
                    if (element.value.lastAccessTime + 20 * 60 * 1000 < now) {
                        iterator.remove()
                        log.info("Session has not been visited for 20 minutes, remove session: $sessionHandle")
                    } else {
                        log.info("SessionId: ${sessionHandle.handleId}")
                    }
                }

                TimeUnit.SECONDS.sleep(3)
            } catch (e: Exception) {
                log.error("Failed to listen for session, error: $e.localizedMessage", e)
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
    ): Session {

        log.info("Server get properties [username:$username, password:$password, sessionConf:$sessionConf]")
        synchronized(sessionAddLock) {
            val session = this.sessionFactory.create(
                username,
                password,
                sessionConf,
                this
            )
            handleToSession[session.sessionHandle] = session

            session.operationManager = operationManager

            log.info("Create session: ${session.sessionHandle}")
            return session
        }
    }

    fun closeSession(sessionHandle: SessionHandle) {
        synchronized(sessionAddLock) {
            handleToSession.remove(sessionHandle) ?: throw BitlapException("Session does not exist: $sessionHandle")
            log.info("Session closed, " + sessionHandle + ", current sessions:" + getOpenSessionCount())
            if (getOpenSessionCount() == 0) {
                log.warn(
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

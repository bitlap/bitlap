/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.session

import com.typesafe.scalalogging.LazyLogging
import org.bitlap.network.NetworkException.ServerIntervalException
import org.bitlap.network.handles.SessionHandle

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import scala.jdk.CollectionConverters.ConcurrentMapHasAsScala

/** bitlap 会话管理器
 *  @author
 *    梦境迷离
 *  @since 2021/11/20
 *  @version 1.0
 */
class SessionManager extends LazyLogging {

  val operationManager: OperationManager = new OperationManager()
  val start                              = new AtomicBoolean(false)

  private lazy val handleToSession: ConcurrentHashMap[SessionHandle, Session] =
    new ConcurrentHashMap[SessionHandle, Session]()

  private lazy val sessionThread: Thread = new Thread { // register center
    override def run(): Unit =
      while (!Thread.currentThread().isInterrupted) {
        logger.info(s"There are [${handleToSession.size}] surviving sessions")
        try {
          handleToSession.asScala.foreach { case (sessionHandle, session) =>
            if (!session.sessionState.get()) {
              handleToSession.remove(sessionHandle)
              logger.info(
                s"Session state is false, remove session: $sessionHandle"
              )
            }

            val now = System.currentTimeMillis()
            if (session.lastAccessTime + 20 * 60 * 1000 < now) {
              handleToSession.remove(sessionHandle)
              logger.info(
                s"Session has not been visited for 20 minutes, remove session: $sessionHandle"
              )
            } else {
              logger.info(s"SessionId: ${sessionHandle.handleId}")
            }
          }

          TimeUnit.SECONDS.sleep(3)
        } catch {
          case e: Exception =>
            logger.error(
              s"Failed to listen for session, error: $e.localizedMessage",
              e
            )
        }
      }
  }

  def startListener(): Unit =
    if (start.compareAndSet(false, true)) {
      sessionThread.setDaemon(true)
      sessionThread.start()
    }

  // service, provider, conf, discover
  // session life cycle manage

  def openSession(
    username: String,
    password: String,
    sessionConf: Map[String, String]
  ): Session = {
    logger.info(
      s"Server get properties [username:$username, password:$password, sessionConf:$sessionConf]"
    )
    SessionManager.sessionAddLock.synchronized {
      val session = new MemorySession(
        username,
        password,
        sessionConf,
        this
      )
      session.open(sessionConf)
      handleToSession.put(session.sessionHandle, session)
      session.operationManager = operationManager
      logger.info(s"Create session: ${session.sessionHandle}")
      return session
    }
  }

  def closeSession(sessionHandle: SessionHandle): Unit = {
    SessionManager.sessionAddLock.synchronized {
      handleToSession.remove(sessionHandle)
    }
    logger.info(
      "Session closed, " + sessionHandle + ", current sessions:" + getOpenSessionCount()
    )
    if (getOpenSessionCount() == 0) {
      //        log.warn(
      //          "This instance of Bitlap has been removed from the list of server " +
      //            "instances available for dynamic service discovery. " +
      //            "The last client session has ended - will shutdown now."
      //        )
    }
  }

  private def getOpenSessionCount(): Int =
    handleToSession.size

  def getSession(sessionHandle: SessionHandle): Session = {
    val session: Session = SessionManager.sessionAddLock.synchronized {
      handleToSession.get(sessionHandle)
    }
    if (session == null) {
      throw ServerIntervalException(s"Invalid SessionHandle: $sessionHandle")
    }
    session
  }

  def refreshSession(sessionHandle: SessionHandle, session: Session): Session =
    SessionManager.sessionAddLock.synchronized {
      session.lastAccessTime = System.currentTimeMillis()
      if (handleToSession.containsKey(sessionHandle)) {
        handleToSession.put(sessionHandle, session)
      } else {
        throw ServerIntervalException(s"Invalid SessionHandle: $sessionHandle")
      }
    }

}
object SessionManager {
  private val sessionAddLock: Object = new Object
}

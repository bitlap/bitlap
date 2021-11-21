package org.bitlap.net.session

import cn.hutool.core.util.ServiceLoaderUtil
import com.typesafe.scalalogging.LazyLogging
import org.bitlap.common.exception.BitlapException
import org.bitlap.net.handles.SessionHandle
import org.bitlap.net.operation.OperationManager

import java.util.concurrent.{ ConcurrentHashMap, TimeUnit }

/**
 *
 * @author 梦境迷离
 * @since 2021/11/20
 * @version 1.0
 */
class SessionManager extends LazyLogging {

  lazy val operationManager: OperationManager = new OperationManager()
  private val sessionFactory: SessionFactory = ServiceLoaderUtil.loadFirst(classOf[SessionFactory])

  private lazy val handleToSession: ConcurrentHashMap[SessionHandle, Session] = new ConcurrentHashMap[SessionHandle, Session]()
  private lazy val sessionAddLock: Object = new Object

  private val sessionThread: Thread = new Thread { // register center
    while (true) {
      import scala.collection.convert.ImplicitConversions.{ `iterator asJava`, `map AsScalaConcurrentMap` }
      val iterator = handleToSession.iterator
      logger.info("There are [${handleToSession.size}] surviving sessions")
      try {
        while (iterator.hasNext) {
          val element = iterator.next()
          val sessionHandle = element._1
          if (!element._2.sessionState.get()) {
            iterator.remove()
            logger.info(s"Session state is false, remove session: $sessionHandle")
          }

          val now = System.currentTimeMillis()
          if (element._2.lastAccessTime + 20 * 60 * 1000 < now) {
            iterator.remove()
            logger.info(s"Session has not been visited for 20 minutes, remove session: $sessionHandle")
          } else {
            logger.info(s"SessionId: ${sessionHandle.handleId}")
          }
        }

        TimeUnit.SECONDS.sleep(3)
      } catch {
        case e: Exception =>
          logger.error("Failed to listen for session, error: $e.localizedMessage", e)
      }
    }
  }
  sessionThread.setDaemon(true)
  sessionThread.start()
  // service, provider, conf, discover
  // session life cycle manage

  def openSession(username: String, password: String, sessionConf: Map[String, String]): Session = {
    logger.info("Server get properties [username:$username, password:$password, sessionConf:$sessionConf]")
    sessionAddLock.synchronized {
      val session = this.sessionFactory.create(
        username,
        password,
        sessionConf,
        this
      )
      handleToSession.put(session.sessionHandle, session)
      session.operationManager = operationManager
      logger.info("Create session: ${session.sessionHandle}")
      return session
    }
  }

  def closeSession(sessionHandle: SessionHandle) = {
    sessionAddLock.synchronized {
      val v = handleToSession.remove(sessionHandle)
      if (v == null) {
        throw new BitlapException("Session does not exist: $sessionHandle")
      } else {
        v
      }
    }
    logger.info("Session closed, " + sessionHandle + ", current sessions:" + getOpenSessionCount())
    if (getOpenSessionCount() == 0) {
      //        log.warn(
      //          "This instance of Bitlap has been removed from the list of server " +
      //            "instances available for dynamic service discovery. " +
      //            "The last client session has ended - will shutdown now."
      //        )
      // TODO STOP server
    }
  }

  private def getOpenSessionCount(): Int = {
    handleToSession.size
  }

  def getSession(sessionHandle: SessionHandle): Session = {
    val session: Session = sessionAddLock.synchronized {
      handleToSession.get(sessionHandle)
    }
    if (session == null) {
      // scala调用kotlin，默认参数被IDE忽略 显示红色。但是maven插件编译是有默认参数的，插件编译通过
      throw new BitlapException("Invalid SessionHandle: $sessionHandle")
    }
    session
  }

  def refreshSession(sessionHandle: SessionHandle, session: Session): Session = {
    sessionAddLock.synchronized {
      session.lastAccessTime = System.currentTimeMillis()
      if (handleToSession.containsKey(sessionHandle)) {
        handleToSession.put(sessionHandle, session)
      } else {
        throw new BitlapException("Invalid SessionHandle: $sessionHandle")
      }
    }
  }

}

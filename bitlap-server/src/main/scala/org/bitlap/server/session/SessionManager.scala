/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.session

import com.typesafe.scalalogging.LazyLogging
import org.bitlap.common.BitlapConf
import org.bitlap.jdbc.BitlapSQLException
import org.bitlap.network.NetworkException.InternalException
import org.bitlap.network.enumeration.{ GetInfoType, OperationState }
import org.bitlap.network.handles.*
import org.bitlap.network.models.GetInfoValue
import org.bitlap.server.BitlapContext
import zio.{ System as _, ZIO, * }
import java.util.Date
import java.util.concurrent.*
import scala.collection.*
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.CollectionHasAsScala
import org.bitlap.server.config.BitlapServerConfiguration

/** bitlap 会话管理器
 *  @author
 *    梦境迷离
 *  @since 2021/11/20
 *  @version 2.0
 */
object SessionManager extends LazyLogging:

  private val sessionAddLock: Object = new Object
  private lazy val sessionStore: ConcurrentHashMap[SessionHandle, Session] =
    new ConcurrentHashMap[SessionHandle, Session]()

  private[session] lazy val opHandleSet = ListBuffer[OperationHandle]()

  private[session] lazy val operationStore: mutable.HashMap[OperationHandle, Operation] =
    mutable.HashMap[OperationHandle, Operation]()

  lazy val live: ULayer[SessionManager] = ZLayer.succeed(new SessionManager())

  /** 启动会话监听，超时时清空会话，清空会话相关的操作缓存
   */
  def startListener(): ZIO[SessionManager & BitlapServerConfiguration, Nothing, Unit] =
    logger.info(s"Bitlap server session listener started, it has [${sessionStore.size}] sessions")
    val current = System.currentTimeMillis
    for {
      sessionConfig <- ZIO.serviceWith[BitlapServerConfiguration](_.sessionConfig)
      sessionTimeout = sessionConfig.timeout.toMillis
      _ <- ZIO
        .foreach(sessionStore.values().asScala) { session =>
          if session.lastAccessTime + sessionTimeout <= current && (session.getNoOperationTime > sessionTimeout) then {
            val handle = session.sessionHandle
            logger.warn(
              s"Session $handle is Timed-out (last access : ${new Date(session.lastAccessTime)}) and will be closed"
            )
            closeSession(handle)
          } else ZIO.attempt(session.removeExpiredOperations(opHandleSet.toList))
        }
        .ignore
      _ <- ZIO.succeed(logger.info(s"Bitlap server has [${sessionStore.size}] sessions"))
    } yield ()

  def openSession(
    username: String,
    password: String,
    sessionConf: Map[String, String]
  ): ZIO[SessionManager, Throwable, Session] =
    ZIO.serviceWithZIO[SessionManager](sm => sm.openSession(username, password, sessionConf))

  def closeSession(sessionHandle: SessionHandle): ZIO[SessionManager, Throwable, Unit] =
    ZIO.serviceWithZIO[SessionManager](sm => sm.closeSession(sessionHandle))

  def getSession(sessionHandle: SessionHandle): ZIO[SessionManager, Throwable, Session] =
    ZIO.serviceWithZIO[SessionManager](sm => sm.getSession(sessionHandle))

  def getOperation(operationHandle: OperationHandle): ZIO[SessionManager, Throwable, Operation] =
    ZIO.serviceWithZIO[SessionManager](sm => sm.getOperation(operationHandle))

  def getInfo(
    sessionHandle: SessionHandle,
    getInfoType: GetInfoType
  ): ZIO[SessionManager, Throwable, GetInfoValue] =
    ZIO.serviceWithZIO[SessionManager](sm => sm.getInfo(sessionHandle, getInfoType))

final class SessionManager extends LazyLogging:
  import SessionManager.*

  def openSession(
    username: String,
    password: String,
    sessionConf: Map[String, String]
  ): Task[Session] =
    ZIO.attemptBlocking {
      SessionManager.sessionAddLock.synchronized {
        val session = new MemorySession(
          username,
          password,
          sessionConf,
          this
        )
        session.open()
        sessionStore.put(session.sessionHandle, session)
        logger.info(s"Create session [${session.sessionHandle}]")
        session
      }
    }

  def closeSession(sessionHandle: SessionHandle): Task[Unit] = ZIO.attemptBlocking {
    SessionManager.sessionAddLock.synchronized {
      sessionStore.remove(sessionHandle)
    }
    val closedOps = new ListBuffer[OperationHandle]()
    for opHandle <- opHandleSet do {
      val op = operationStore.getOrElse(opHandle, null)
      if op != null then {
        op.setState(OperationState.ClosedState)
        operationStore.remove(opHandle)
      }
      closedOps.append(opHandle)
    }
    closedOps.zipWithIndex.foreach { case (_, i) =>
      opHandleSet.remove(i)
    }
    logger.info(
      s"Close session [$sessionHandle], [${getOpenSessionCount}] sessions exists"
    )
  }

  def getSession(sessionHandle: SessionHandle): Task[Session] = ZIO.attemptBlocking {
    this.synchronized {
      val session: Session = SessionManager.sessionAddLock.synchronized {
        sessionStore.get(sessionHandle)
      }
      if session == null then {
        throw InternalException(s"Invalid SessionHandle: $sessionHandle")
      }
      refreshSession(sessionHandle, session)
      session
    }
  }

  def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): Task[GetInfoValue] = ZIO.attemptBlocking {
    this.synchronized {
      val session: Session = SessionManager.sessionAddLock.synchronized {
        sessionStore.get(sessionHandle)
      }
      if session == null then {
        throw InternalException(s"Invalid SessionHandle: $sessionHandle")
      }
      refreshSession(sessionHandle, session)
      session.getInfo(getInfoType)
    }
  }

  def getOperation(operationHandle: OperationHandle): Task[Operation] = ZIO.attemptBlocking {
    this.synchronized {
      val op = operationStore.getOrElse(operationHandle, null)
      if op == null then {
        throw BitlapSQLException(s"Invalid OperationHandle: $operationHandle")
      } else {
        refreshSession(op.parentSession.sessionHandle, op.parentSession)
        op.state match {
          case OperationState.FinishedState => op
          case _ =>
            throw BitlapSQLException(s"Invalid OperationState: ${op.state}")
        }
      }
    }
  }

  private def getOpenSessionCount: Int =
    sessionStore.size

  private def refreshSession(sessionHandle: SessionHandle, session: Session): Session =
    SessionManager.sessionAddLock.synchronized {
      session.asInstanceOf[MemorySession]._lastAccessTime = System.currentTimeMillis()
      if sessionStore.containsKey(sessionHandle) then {
        sessionStore.put(sessionHandle, session)
      } else {
        throw InternalException(s"Invalid SessionHandle: $sessionHandle")
      }
    }
  end refreshSession

end SessionManager

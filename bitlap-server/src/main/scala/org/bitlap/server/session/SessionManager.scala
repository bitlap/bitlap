/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.session

import com.typesafe.scalalogging.LazyLogging
import org.bitlap.common.BitlapConf
import org.bitlap.jdbc.BitlapSQLException
import org.bitlap.network.NetworkException.InternalException
import org.bitlap.network.OperationState
import org.bitlap.network.handles._
import org.bitlap.server.BitlapContext
import org.bitlap.server.session.SessionManager._
import zio.blocking.Blocking
import zio.{ Has, Task, ZIO, ZLayer }

import java.util.Date
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection._
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.CollectionHasAsScala

/** bitlap 会话管理器
 *  @author
 *    梦境迷离
 *  @since 2021/11/20
 *  @version 2.0
 */
object SessionManager {

  private val timeoutCheckerLock     = new Object
  private val sessionAddLock: Object = new Object
  lazy val live: ZLayer[Blocking, Nothing, Has[SessionManager]] =
    ZLayer.fromService((block: Blocking.Service) => new SessionManager(block))

  def openSession(
    username: String,
    password: String,
    sessionConf: Map[String, String]
  ): ZIO[Has[SessionManager], Throwable, Session] =
    ZIO.serviceWith[SessionManager](sm => sm.openSession(username, password, sessionConf))

  def closeSession(sessionHandle: SessionHandle): ZIO[Has[SessionManager], Throwable, Unit] =
    ZIO.serviceWith[SessionManager](sm => sm.closeSession(sessionHandle))

  def getSession(sessionHandle: SessionHandle): ZIO[Has[SessionManager], Throwable, Session] =
    ZIO.serviceWith[SessionManager](sm => sm.getSession(sessionHandle))

  def getOperation(operationHandle: OperationHandle): ZIO[Has[SessionManager], Throwable, Operation] =
    ZIO.serviceWith[SessionManager](sm => sm.getOperation(operationHandle))

  def startListener(): ZIO[Has[SessionManager], Throwable, Unit] =
    ZIO.serviceWith[SessionManager](sm => sm.startListener())

}
final class SessionManager(block: Blocking.Service) extends LazyLogging {

  private val start = new AtomicBoolean(false)

  private[session] lazy val opHandleSet = ListBuffer[OperationHandle]()

  private[session] lazy val operationStore: mutable.HashMap[OperationHandle, Operation] =
    mutable.HashMap[OperationHandle, Operation]()

  private lazy val sessionStore: ConcurrentHashMap[SessionHandle, Session] =
    new ConcurrentHashMap[SessionHandle, Session]()

  private def sleepFor(interval: Long): Unit =
    timeoutCheckerLock.synchronized {
      try timeoutCheckerLock.wait(interval)
      catch {
        case _: InterruptedException =>
      }

    }

  private final val sessionTimeout = Duration(BitlapContext.globalConf.get(BitlapConf.SESSION_TIMEOUT)).toMillis
  private final val interval       = 3000

  private lazy val sessionThread: Thread = new Thread {
    override def run(): Unit = {
      sleepFor(interval)
      while (!Thread.currentThread().isInterrupted) {
        logger.info(s"[${sessionStore.size}] sessions exists")
        val current = System.currentTimeMillis
        for (session <- sessionStore.values().asScala)
          if (session.lastAccessTime + sessionTimeout <= current && (session.getNoOperationTime > sessionTimeout)) {
            val handle = session.sessionHandle
            logger.warn(
              s"Session $handle is Timed-out (last access : ${new Date(session.lastAccessTime)}) and will be closed"
            )
            try zio.Runtime.default.unsafeRun(closeSession(handle))
            catch {
              case e: Exception =>
                logger.warn("Exception is thrown closing session " + handle, e)
            }
          } else session.removeExpiredOperations(opHandleSet.toList)
        sleepFor(interval)
      }
    }
  }

  def startListener(): Task[Unit] = Task.effect {
    if (start.compareAndSet(false, true)) {
      sessionThread.setDaemon(true)
      sessionThread.start()
    }
  }

  def openSession(
    username: String,
    password: String,
    sessionConf: Map[String, String]
  ): Task[Session] =
    block.effectBlocking {
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

  def closeSession(sessionHandle: SessionHandle): Task[Unit] = block.effectBlocking {
    SessionManager.sessionAddLock.synchronized {
      sessionStore.remove(sessionHandle)
    }
    val closedOps = new ListBuffer[OperationHandle]()
    for (opHandle <- opHandleSet) {
      val op = operationStore.getOrElse(opHandle, null)
      if (op != null) {
        op.setState(OperationState.ClosedState)
        operationStore.remove(opHandle)
      }
      closedOps.append(opHandle)
    }
    closedOps.zipWithIndex.foreach { case (_, i) =>
      opHandleSet.remove(i)
    }
    logger.info(
      s"Close session [$sessionHandle], [${getOpenSessionCount()}] sessions exists"
    )
  }

  def getSession(sessionHandle: SessionHandle): Task[Session] = block.effectBlocking {
    this.synchronized {
      val session: Session = SessionManager.sessionAddLock.synchronized {
        sessionStore.get(sessionHandle)
      }
      if (session == null) {
        throw InternalException(s"Invalid SessionHandle: $sessionHandle")
      }
      refreshSession(sessionHandle, session)
      session
    }
  }

  def getOperation(operationHandle: OperationHandle): Task[Operation] = block.effectBlocking {
    this.synchronized {
      val op = operationStore.getOrElse(operationHandle, null)
      if (op == null) {
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

  private def getOpenSessionCount(): Int =
    sessionStore.size

  private def refreshSession(sessionHandle: SessionHandle, session: Session): Session =
    SessionManager.sessionAddLock.synchronized {
      session.lastAccessTime = System.currentTimeMillis()
      if (sessionStore.containsKey(sessionHandle)) {
        sessionStore.put(sessionHandle, session)
      } else {
        throw InternalException(s"Invalid SessionHandle: $sessionHandle")
      }
    }
}

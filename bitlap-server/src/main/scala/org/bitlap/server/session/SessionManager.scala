/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.server.session

import java.util.Date
import java.util.concurrent.*

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.CollectionHasAsScala

import org.bitlap.common.BitlapConf
import org.bitlap.network.NetworkException.InternalException
import org.bitlap.network.enumeration.{ GetInfoType, OperationState }
import org.bitlap.network.handles.*
import org.bitlap.network.models.GetInfoValue
import org.bitlap.server.BitlapContext
import org.bitlap.server.config.BitlapServerConfiguration

import com.typesafe.scalalogging.LazyLogging

import zio.{ System as _, ZIO, * }

/** Bitlap session manager
 */
object SessionManager extends LazyLogging:

  private val sessionAddLock: Object = new Object

  private lazy val sessionStore: ConcurrentHashMap[SessionHandle, Session] =
    new ConcurrentHashMap[SessionHandle, Session]()

  private[session] lazy val opHandleSet = ListBuffer[OperationHandle]()

  private[session] lazy val operationStore: mutable.HashMap[OperationHandle, Operation] =
    mutable.HashMap[OperationHandle, Operation]()

  lazy val live: ULayer[SessionManager] = ZLayer.succeed(new SessionManager())

  /** Start session listening, clear session when timeout occurs, and clear session related operation cache
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
        val session = new SimpleLocalSession(
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
      s"Close session [$sessionHandle], [${sessionStore.size}] sessions exists"
    )
  }

  def getSession(sessionHandle: SessionHandle): Task[Session] =
    default.withLocalSession(sessionHandle) { session =>
      ZIO.succeed(session)
    }

  def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): Task[GetInfoValue] =
    default.withLocalSession(sessionHandle) { session =>
      session.getInfo(getInfoType)
    }

  def getOperation(operationHandle: OperationHandle): Task[Operation] =
    default.withOperation(operationHandle) { operation =>
      operation.state match {
        case OperationState.FinishedState => ZIO.succeed(operation)
        case _ =>
          ZIO.fail(InternalException(s"Invalid OperationState: ${operation.state}"))
      }
    }

  private def refreshSession(sessionHandle: SessionHandle, session: Session): Session =
    SessionManager.sessionAddLock.synchronized {
      session.asInstanceOf[SimpleLocalSession].lastAccessTime = System.currentTimeMillis()
      if sessionStore.containsKey(sessionHandle) then {
        sessionStore.put(sessionHandle, session)
      } else {
        throw InternalException(s"Invalid SessionHandle: $sessionHandle")
      }
    }
  end refreshSession

  trait SessionDSL[F[_]] {
    def withOperation[A](operationHandle: OperationHandle)(effect: Operation => F[A]): F[A]
    def withLocalSession[A](sessionHandle: SessionHandle)(effect: Session => F[A]): F[A]
  }

  // public session operation
  lazy val default: SessionDSL[Task] = new SessionDSL[Task] {

    def withOperation[A](operationHandle: OperationHandle)(effect: Operation => Task[A]): Task[A] =
      val op = operationStore.getOrElse(operationHandle, null)
      if op == null then {
        ZIO.fail(InternalException(s"Invalid OperationHandle: $operationHandle"))
      } else {
        refreshSession(op.parentSession.sessionHandle, op.parentSession)
        effect(op)
      }

    def withLocalSession[A](sessionHandle: SessionHandle)(effect: Session => Task[A]): Task[A] =
      ZIO.attemptBlocking {
        // TODO
        SessionManager.sessionAddLock.synchronized {
          val session = sessionStore.get(sessionHandle)
          if session == null then {
            ZIO.fail(InternalException(s"Invalid SessionHandle: $sessionHandle"))
          } else {
            refreshSession(sessionHandle, session)
            effect(session)
          }
        }
      }.flatten
  }

end SessionManager

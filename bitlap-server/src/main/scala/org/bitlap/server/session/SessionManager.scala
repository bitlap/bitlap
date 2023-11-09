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
import java.util.Vector as JVector
import java.util.concurrent.*

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.CollectionHasAsScala

import org.bitlap.common.BitlapConf
import org.bitlap.common.exception.BitlapException
import org.bitlap.network.enumeration.{ GetInfoType, OperationState }
import org.bitlap.network.handles.*
import org.bitlap.network.models.GetInfoValue
import org.bitlap.server.config.BitlapConfiguration

import zio.{ System as _, * }
import zio.Ref.Synchronized

/** Bitlap session manager
 */
object SessionManager:

  // use zio ref?
  private[session] lazy val SessionStoreMap: ConcurrentHashMap[SessionHandle, Session] =
    new ConcurrentHashMap[SessionHandle, Session]()

  private[session] lazy val OperationHandleVector = new JVector[OperationHandle]()

  private[session] lazy val OperationStoreMap: ConcurrentHashMap[OperationHandle, Operation] =
    ConcurrentHashMap[OperationHandle, Operation]()

  lazy val live: ULayer[SessionManager] = ZLayer.succeed(new SessionManager())

  /** Start session listening, clear session when timeout occurs, and clear session related operation cache
   */
  def startListener(): ZIO[SessionManager & BitlapConfiguration, Nothing, Unit] =
    for {
      _             <- ZIO.logInfo(s"Bitlap server session listener started, it has [${SessionStoreMap.size}] sessions")
      now           <- Clock.currentTime(TimeUnit.MILLISECONDS)
      sessionConfig <- ZIO.serviceWith[BitlapConfiguration](_.sessionConfig)
      sessionTimeout = sessionConfig.timeout.toMillis
      _ <- ZIO
        .foreach(SessionStoreMap.values().asScala) { session =>
          if session.lastAccessTime + sessionTimeout <= now && (session.getNoOperationTime > sessionTimeout) then {
            val handle = session.sessionHandle
            ZIO.logWarning(
              s"Session $handle is Timed-out (last access : ${new Date(session.lastAccessTime)}) and will be closed"
            ) *> ZIO.serviceWithZIO[SessionManager](sm => sm.closeSession(handle))
          } else ZIO.attempt(session.removeExpiredOperations(OperationHandleVector.asScala.toList))
        }
        .ignore
      _ <- ZIO.logInfo(s"Bitlap server has [${SessionStoreMap.size}] sessions")
    } yield ()

  end startListener

  trait SessionDSL[F[_]] {
    def withOperation[A](operationHandle: OperationHandle)(effect: Operation => F[A]): F[A]

    def withLocalSession[A](sessionHandle: SessionHandle)(effect: Session => F[A]): F[A]
  }

  private def refreshSession(sessionHandle: SessionHandle, session: Session): Task[Session] =
    ZIO.attemptBlocking {
      // TODO remove synchronized, use Ref or Synchronized?
      session.asInstanceOf[SimpleLocalSession].lastAccessTime = System.currentTimeMillis()
      if SessionStoreMap.containsKey(sessionHandle) then {
        SessionStoreMap.put(sessionHandle, session)
      } else {
        throw BitlapException(s"Invalid SessionHandle: $sessionHandle")
      }
    }
  end refreshSession

  // public session operation
  lazy val default: SessionDSL[Task] = new SessionDSL[Task] {

    def withOperation[A](operationHandle: OperationHandle)(effect: Operation => Task[A]): Task[A] =
      ZIO.attemptBlocking {
        val op = OperationStoreMap.getOrDefault(operationHandle, null)
        if op == null then {
          throw BitlapException(s"Invalid OperationHandle: $operationHandle")
        } else {
          refreshSession(op.parentSession.sessionHandle, op.parentSession) *> effect(op)
        }
      }.flatten

    def withLocalSession[A](sessionHandle: SessionHandle)(effect: Session => Task[A]): Task[A] =
      ZIO.attemptBlocking {
        val session = SessionStoreMap.get(sessionHandle)
        if session == null then {
          throw BitlapException(s"Invalid SessionHandle: $sessionHandle")
        } else {
          refreshSession(sessionHandle, session) *> effect(session)
        }
      }.flatten
  }

end SessionManager

final class SessionManager:
  import SessionManager.*

  def openSession(
    username: String,
    password: String,
    sessionConf: Map[String, String]
  ): Task[Session] =
    ZIO.attemptBlocking {
      val session = new SimpleLocalSession(
        username,
        password,
        sessionConf,
        this
      )
      session.open()
      SessionStoreMap.put(session.sessionHandle, session)
      session
    }.tap(session => ZIO.logInfo(s"Create session [${session.sessionHandle}]"))

  def closeSession(sessionHandle: SessionHandle): Task[Unit] = ZIO.attemptBlocking {
    SessionStoreMap.remove(sessionHandle)
    val closedOps = new ListBuffer[OperationHandle]()
    for opHandle <- OperationHandleVector.asScala do {
      val op = OperationStoreMap.getOrDefault(opHandle, null)
      if op != null then {
        op.setState(OperationState.ClosedState)
        OperationStoreMap.remove(opHandle)
      }
      closedOps.append(opHandle)
    }
    closedOps.foreach { clp =>
      OperationHandleVector.remove(clp)
    }
  } *> ZIO.logInfo(
    s"Close session [$sessionHandle], [${SessionStoreMap.size}] sessions exists"
  )

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
          ZIO.fail(BitlapException(s"Invalid OperationState: ${operation.state}"))
      }
    }

end SessionManager

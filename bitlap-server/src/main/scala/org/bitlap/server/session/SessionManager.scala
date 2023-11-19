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
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicLong, AtomicReference }

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.CollectionHasAsScala

import org.bitlap.common.BitlapConf
import org.bitlap.common.exception.BitlapException
import org.bitlap.core.catalog.metadata.Database
import org.bitlap.network.enumeration.{ GetInfoType, OperationState }
import org.bitlap.network.handles.*
import org.bitlap.network.models.GetInfoValue
import org.bitlap.server.BitlapGlobalContext
import org.bitlap.server.config.BitlapConfiguration

import zio.{ System as _, * }

/** Bitlap session manager
 */
object SessionManager:

  val live: ZLayer[BitlapGlobalContext, Nothing, SessionManager] =
    ZLayer.fromFunction((in: BitlapGlobalContext) => new SessionManager(using in))

end SessionManager

final class SessionManager(using globalContext: BitlapGlobalContext):
  import SessionManager.*

  /** Start session listening, clear session when timeout occurs, and clear session related operation cache
   */
  def startListener(): ZIO[Any, Nothing, Unit] = {
    for {
      sessionStoreMap       <- globalContext.sessionStoreMap.get
      operationHandleVector <- globalContext.operationHandleVector.get
      _                     <- ZIO.logInfo(s"Session state check started: ${sessionStoreMap.size} sessions")
      now                   <- Clock.currentTime(TimeUnit.MILLISECONDS)
      sessionConfig  = globalContext.config.sessionConfig
      sessionTimeout = sessionConfig.timeout.toMillis
      _ <- ZIO
        .foreach(sessionStoreMap.values().asScala) { session =>
          for {
            lastAccessTime  <- session.lastAccessTimeRef.get.map(_.get())
            noOperationTime <- session.getNoOperationTime
            re <- {
              if lastAccessTime + sessionTimeout <= now && (noOperationTime > sessionTimeout) then {
                val handle = session.sessionHandle
                ZIO.logWarning(
                  s"Session $handle is Timed-out (last access : ${new Date(lastAccessTime)}) and will be closed"
                ) *> closeSession(handle)
              } else session.removeExpiredOperations(operationHandleVector.asScala.toList).unit
            }
          } yield re
        }
        .ignoreLogged
      _ <- ZIO.logInfo(s"Session state check ended: ${sessionStoreMap.size} sessions")
    } yield ()
  }

  def openSession(
    username: String,
    password: String,
    sessionConf: Map[String, String]
  ): Task[Session] =
    for {
      sessionStoreMap       <- globalContext.sessionStoreMap.get
      operationStoreMap     <- globalContext.operationStoreMap.get
      operationHandleVector <- globalContext.operationHandleVector.get
      sessionState          <- Ref.make(new AtomicBoolean(true))
      sessionCreateTime     <- Ref.make(new AtomicLong(System.currentTimeMillis()))
      defaultSessionConf    <- Ref.make(mutable.Map(sessionConf.toList: _*))
      defaultSchema         <- Ref.make(AtomicReference(Database.DEFAULT_DATABASE))
      session <- ZIO
        .attempt(
          new SimpleLocalSession(
            username = username,
            password = password,
            sessionManager = this,
            sessionConfRef = defaultSessionConf,
            sessionStateRef = sessionState,
            creationTimeRef = sessionCreateTime,
            lastAccessTimeRef = sessionCreateTime,
            defaultSchema
          )(using sessionStoreMap, operationHandleVector, operationStoreMap, globalContext.config)
        )
        .tap(s => ZIO.succeed(sessionStoreMap.put(s.sessionHandle, s)))
      _ <- ZIO.logInfo(s"Create session [${session.sessionHandle}]")
    } yield session

  def closeSession(sessionHandle: SessionHandle): Task[Unit] =
    for {
      sessionStoreMap       <- globalContext.sessionStoreMap.get
      operationHandleVector <- globalContext.operationHandleVector.get
      operationStoreMap     <- globalContext.operationStoreMap.get
      _ <- ZIO.attemptBlocking {
        sessionStoreMap.remove(sessionHandle)
        val closedOps = new ListBuffer[OperationHandle]()
        for opHandle <- operationHandleVector.asScala do {
          val op = operationStoreMap.getOrDefault(opHandle, null)
          if op != null then {
            op.setState(OperationState.ClosedState)
            operationStoreMap.remove(opHandle)
          }
          closedOps.append(opHandle)
        }
        closedOps.foreach { clp =>
          operationHandleVector.remove(clp)
        }
      }
      _ <- ZIO.logInfo(s"Close session [$sessionHandle], [${sessionStoreMap.size}] sessions exists")
    } yield ()

  def getSession(sessionHandle: SessionHandle): Task[Session] =
    for {
      sessionStoreMap <- globalContext.sessionStoreMap.get
      re <- ZIO.attemptBlocking {
        val session = sessionStoreMap.get(sessionHandle)
        if session == null then {
          ZIO.fail(BitlapException(s"Invalid SessionHandle: $sessionHandle"))
        } else {
          refreshSession(sessionHandle, session) *> ZIO.succeed(session)
        }
      }.flatten
    } yield re

  def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): Task[GetInfoValue] =
    for {
      sessionStoreMap <- globalContext.sessionStoreMap.get
      re <- ZIO.attemptBlocking {
        val session = sessionStoreMap.get(sessionHandle)
        if session == null then {
          ZIO.fail(BitlapException(s"Invalid SessionHandle: $sessionHandle"))
        } else {
          refreshSession(sessionHandle, session) *> session.getInfo(getInfoType)
        }
      }.flatten
    } yield re

  def getOperation(operationHandle: OperationHandle): Task[Operation] =
    for {
      operationStoreMap <- globalContext.operationStoreMap.get
      re <- ZIO.attemptBlocking {
        val op = operationStoreMap.getOrDefault(operationHandle, null)
        if op == null then {
          ZIO.fail(BitlapException(s"Invalid OperationHandle: $operationHandle"))
        } else {
          refreshSession(op.parentSession.sessionHandle, op.parentSession) *> {
            op.state match {
              case OperationState.FinishedState => ZIO.succeed(op)
              case _ =>
                ZIO.fail(BitlapException(s"Invalid OperationState: ${op.state}"))
            }
          }
        }
      }.flatten
    } yield re

  private def refreshSession(sessionHandle: SessionHandle, session: Session): Task[Session] =
    for {
      sessionStoreMap <- globalContext.sessionStoreMap.get
      _ <- session.asInstanceOf[SimpleLocalSession].lastAccessTimeRef.updateAndGet { lt =>
        lt.set(System.currentTimeMillis())
        if sessionStoreMap.containsKey(sessionHandle) then {
          sessionStoreMap.put(sessionHandle, session)
        }
        lt
      }
    } yield session
end SessionManager

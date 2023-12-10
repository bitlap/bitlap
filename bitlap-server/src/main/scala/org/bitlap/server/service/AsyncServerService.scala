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
package org.bitlap.server.service

import scala.util.control.NonFatal

import org.bitlap.common.exception._
import org.bitlap.core.catalog.metadata.Database
import org.bitlap.network.*
import org.bitlap.network.enumeration.*
import org.bitlap.network.handles.*
import org.bitlap.network.models.*
import org.bitlap.network.protocol.AsyncProtocol
import org.bitlap.server.session.SessionManager

import com.typesafe.scalalogging.LazyLogging

import zio.*

/** Server side implementation of asynchronous RPC.
 */
object AsyncServerService:

  lazy val live: ZLayer[SessionManager, Nothing, AsyncProtocol] =
    ZLayer.fromFunction((sm: SessionManager) => new AsyncServerService(sm))
end AsyncServerService

final class AsyncServerService(sessionManager: SessionManager) extends AsyncProtocol with LazyLogging:

  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String] = Map.empty
  ): Task[SessionHandle] =
    (for {
      session <- sessionManager.openSession(username, password, configuration)
      _ <- session.currentSchemaRef.updateAndGet { currentSchema =>
        currentSchema.set(configuration.getOrElse("DBNAME", Database.DEFAULT_DATABASE))
        currentSchema
      }
    } yield session.sessionHandle)
      .onError(ce => ZIO.logErrorCause(ce).ignoreLogged)

  override def closeSession(sessionHandle: SessionHandle): Task[Unit] =
    sessionManager.closeSession(sessionHandle)

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String] = Map.empty
  ): Task[OperationHandle] =
    sessionManager
      .getSession(sessionHandle)
      .flatMap {
        _.executeStatement(
          statement.stripSuffix(";"),
          confOverlay,
          queryTimeout
        ).mapError {
          case throwable: BitlapThrowable =>
            logger.error(s"Internal Error: $statement", throwable)
            throwable
          case NonFatal(e) =>
            logger.error(s"Unknown Error: $statement", e)
            BitlapException(s"Unknown Error: ${e.getLocalizedMessage}", cause = Option(e))
        }.onError(ce => ZIO.logErrorCause(ce).ignoreLogged)
      }

  override def fetchResults(
    opHandle: OperationHandle,
    maxRows: Int,
    fetchType: Int
  ): Task[FetchResults] =
    (for {
      operation <- sessionManager
        .getOperation(opHandle)
      rs <- operation.parentSession.fetchResults(operation.opHandle)
    } yield FetchResults(hasMoreRows = false, rs)).onError(ce => ZIO.logErrorCause(ce).ignoreLogged)

  override def getResultSetMetadata(opHandle: OperationHandle): Task[TableSchema] =
    (for {
      operation <- sessionManager.getOperation(opHandle)
      rs        <- operation.parentSession.getResultSetMetadata(opHandle)
    } yield rs).onError(ce => ZIO.logErrorCause(ce).ignoreLogged)

  override def cancelOperation(opHandle: OperationHandle): Task[Unit] =
    sessionManager
      .getOperation(opHandle)
      .flatMap { operation =>
        val session = operation.parentSession
        session.cancelOperation(opHandle)
      }
      .onError(ce => ZIO.logErrorCause(ce).ignoreLogged)

  override def closeOperation(opHandle: OperationHandle): Task[Unit] =
    sessionManager
      .getOperation(opHandle)
      .flatMap { operation =>
        val session = operation.parentSession
        session.closeOperation(opHandle)
      }
      .onError(ce => ZIO.logErrorCause(ce).ignoreLogged)

  override def getOperationStatus(opHandle: OperationHandle): Task[OperationStatus] =
    ZIO
      .succeed(OperationStatus(Some(true), Some(OperationState.FinishedState)))
      .onError(ce => ZIO.logErrorCause(ce).ignoreLogged)

  override def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): Task[GetInfoValue] =
    sessionManager
      .getInfo(sessionHandle, getInfoType)
      .onError(ce => ZIO.logErrorCause(ce).ignoreLogged)

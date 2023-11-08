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

import org.bitlap.common.exception.BitlapException
import org.bitlap.jdbc.Constants
import org.bitlap.network.*
import org.bitlap.network.enumeration.*
import org.bitlap.network.handles.*
import org.bitlap.network.models.*
import org.bitlap.network.protocol.AsyncProtocol
import org.bitlap.server.session.SessionManager

import com.typesafe.scalalogging.LazyLogging

import zio.*

/** Server side implementation of asynchronous RPC
 */
object DriverService:
  lazy val live: ULayer[AsyncProtocol] = ZLayer.succeed(new DriverService)
end DriverService

final class DriverService extends AsyncProtocol with LazyLogging:

  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String] = Map.empty
  ): ZIO[Any, Throwable, SessionHandle] =
    SessionManager
      .openSession(username, password, configuration)
      .map { session =>
        session.currentSchema = configuration.getOrElse(Constants.DBNAME_PROPERTY_KEY, Constants.DEFAULT_DB)
        session.sessionHandle
      }
      .provide(SessionManager.live)

  override def closeSession(sessionHandle: SessionHandle): ZIO[Any, Throwable, Unit] =
    SessionManager.closeSession(sessionHandle).provide(SessionManager.live)

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String] = Map.empty
  ): ZIO[Any, Throwable, OperationHandle] =
    SessionManager
      .getSession(sessionHandle)
      .mapAttempt { session =>
        try
          session.executeStatement(
            statement,
            confOverlay,
            queryTimeout
          )
        catch {
          case bitlapException: BitlapException =>
            logger.error(s"Internal Error: $statement", bitlapException)
            throw bitlapException
          case e: Throwable =>
            logger.error(s"Unknown Error: $statement", e)
            throw BitlapException(s"Unknown Error: ${e.getLocalizedMessage}", cause = Option(e))
        }
      }
      .provide(SessionManager.live)

  override def fetchResults(
    opHandle: OperationHandle,
    maxRows: Int,
    fetchType: Int
  ): ZIO[Any, Throwable, FetchResults] =
    (for {
      operation <- SessionManager
        .getOperation(opHandle)
      rs <- operation.parentSession.fetchResults(operation.opHandle)
    } yield FetchResults(hasMoreRows = false, rs)).provide(SessionManager.live)

  override def getResultSetMetadata(opHandle: OperationHandle): ZIO[Any, Throwable, TableSchema] =
    (for {
      operation <- SessionManager.getOperation(opHandle)
      rs        <- operation.parentSession.getResultSetMetadata(opHandle)
    } yield rs).provide(SessionManager.live)

  override def cancelOperation(opHandle: OperationHandle): Task[Unit] =
    SessionManager
      .getOperation(opHandle)
      .map { operation =>
        val session = operation.parentSession
        session.cancelOperation(opHandle)
      }
      .provide(SessionManager.live)

  override def closeOperation(opHandle: OperationHandle): Task[Unit] =
    SessionManager
      .getOperation(opHandle)
      .map { operation =>
        val session = operation.parentSession
        session.closeOperation(opHandle)
      }
      .provide(SessionManager.live)

  override def getOperationStatus(opHandle: OperationHandle): Task[OperationStatus] =
    ZIO.succeed(OperationStatus(Some(true), Some(OperationState.FinishedState)))

  override def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): Task[GetInfoValue] =
    SessionManager
      .getInfo(sessionHandle, getInfoType)
      .provide(SessionManager.live)

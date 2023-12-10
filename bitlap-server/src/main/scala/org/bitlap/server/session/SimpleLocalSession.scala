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

import java.util.concurrent.atomic.*

import scala.collection.mutable

import org.bitlap.common.BitlapVersionInfo
import org.bitlap.common.exception.BitlapSQLException
import org.bitlap.network.enumeration.*
import org.bitlap.network.enumeration.GetInfoType.*
import org.bitlap.network.handles.*
import org.bitlap.network.models.*
import org.bitlap.server.BitlapGlobalContext
import org.bitlap.server.config.BitlapConfigWrapper

import com.google.protobuf.ByteString
import com.typesafe.scalalogging.StrictLogging

import zio.{ System as _, * }
import zio.Ref.Synchronized

/** Bitlap session implementation on a single machine
 */
final class SimpleLocalSession(
  val getOperation: OperationHandle => Task[Operation],
  val sessionHandle: SessionHandle = SessionHandle(HandleIdentifier()),
  val sessionConfRef: Synchronized[mutable.Map[String, String]],
  val sessionStateRef: Ref[AtomicBoolean],
  val creationTimeRef: Ref[AtomicLong],
  val lastAccessTimeRef: Ref[AtomicLong],
  val currentSchemaRef: Ref[AtomicReference[String]]
)(using globalContext: BitlapGlobalContext)
    extends Session
    with StrictLogging {

  given BitlapConfigWrapper = globalContext.config

  override def executeStatement(
    statement: String,
    confOverlay: Map[String, String]
  ): Task[OperationHandle] =
    for {
      sc <- sessionConfRef.get
      re <-
        newExecuteStatementOperation(
          this,
          statement,
          sc ++ confOverlay
        ).map(_.opHandle)

    } yield re

  override def executeStatement(
    statement: String,
    confOverlay: Map[String, String],
    queryTimeout: Long
  ): Task[OperationHandle] =
    executeStatement(statement, confOverlay)

  override def fetchResults(
    operationHandle: OperationHandle
  ): Task[RowSet] =
    getOperation(operationHandle).map { op =>
      val rows = op.getNextResultSet()
      op.remove(operationHandle) // TODO (work with fetch offset & size)
      rows
    }

  override def getResultSetMetadata(
    operationHandle: OperationHandle
  ): Task[TableSchema] =
    getOperation(operationHandle).map(_.getResultSetSchema())

  override def closeOperation(operationHandle: OperationHandle): Task[Unit] =
    for {
      sessionManager    <- globalContext.getSessionManager
      operationStoreMap <- sessionManager.operations.get
      _ <- ZIO.attemptBlocking {
        val op = operationStoreMap.getOrDefault(operationHandle, null)
        {
          op.setState(OperationState.ClosedState)
          removeOperation(operationHandle)
        }.unless(op == null)
      }
    } yield ()

  override def cancelOperation(operationHandle: OperationHandle): Task[Unit] =
    for {
      sessionManager    <- globalContext.getSessionManager
      operationStoreMap <- sessionManager.operations.get
      re <- ZIO.attemptBlocking {
        val op = operationStoreMap.getOrDefault(operationHandle, null)
        if op != null then {
          if op.state.terminal then {
            ZIO.logInfo(s"$operationHandle Operation is already aborted in state - ${op.state}")
          } else {
            ZIO.attemptBlocking {
              op.setState(OperationState.CanceledState)
            } *>
              ZIO.logInfo(s"$operationHandle Attempting to cancel from state - ${op.state}") *> removeOperation(
                operationHandle
              )
          }
        }
      }
    } yield re

  override def removeExpiredOperations(handles: List[OperationHandle]): Task[List[Operation]] = {
    ZIO.blocking(
      ZIO.foreach(handles)(handle => removeTimedOutOperation(handle)).map(_.map(_.toList)).map(_.flatten)
    )
  }

  /** Create an operation for the SQL and execute it. For now, we put the results in memory by Map.
   */
  private def newExecuteStatementOperation(
    parentSession: Session,
    statement: String,
    confOverlay: scala.collection.Map[String, String] = Map.empty
  ): Task[Operation] =
    for {
      sessionManager        <- globalContext.getSessionManager
      operationHandleVector <- sessionManager.operationIds.get
      operationStoreMap     <- sessionManager.operations.get
      re <- ZIO.attempt {
        val operation = new SimpleOperation(
          parentSession,
          OperationType.ExecuteStatement,
          hasResultSet = true
        )
        confOverlay.foreach(kv => operation.confOverlay.put(kv._1, kv._2))
        operationHandleVector.add(operation.opHandle)
        operationStoreMap.put(operation.opHandle, operation)
        operation.statement = statement
        operation.run().as(operation)
      }.flatten

    } yield re

  private def removeOperation(operationHandle: OperationHandle): Task[Option[Operation]] =
    for {
      sessionManager        <- globalContext.getSessionManager
      operationHandleVector <- sessionManager.operationIds.get
      operationStoreMap     <- sessionManager.operations.get
      re <- ZIO.attemptBlocking {
        val r = operationStoreMap.remove(operationHandle)
        operationHandleVector.remove(operationHandle)
        Option(r)
      }
    } yield re

  private def removeTimedOutOperation(operationHandle: OperationHandle): Task[Option[Operation]] = {
    for {
      sessionManager    <- globalContext.getSessionManager
      operationStoreMap <- sessionManager.operations.get
      operation         <- ZIO.succeed(operationStoreMap.get(operationHandle))
      re <-
        if operation != null && operation.isTimedOut(System.currentTimeMillis) then {
          removeOperation(operationHandle)
        } else ZIO.succeed(Option(operation))
    } yield re
  }

  override def getNoOperationTime: Task[Long] = {
    for {
      sessionManager        <- globalContext.getSessionManager
      lt                    <- lastAccessTimeRef.get.map(_.get())
      operationHandleVector <- sessionManager.operationIds.get
      re <- ZIO.attempt {
        val noMoreOpHandle = operationHandleVector.isEmpty
        if noMoreOpHandle then System.currentTimeMillis - lt
        else 0
      }
    } yield re
  }

  override def getInfo(getInfoType: GetInfoType): Task[GetInfoValue] =
    ZIO.attempt {
      getInfoType match {
        case ServerName =>
          Some(GetInfoValue(ByteString.copyFromUtf8("Bitlap")))
        case ServerConf =>
          Some(GetInfoValue(ByteString.copyFromUtf8(org.bitlap.core.BitlapContext.bitlapConf.toJson)))
        case DbmsName =>
          Some(GetInfoValue(ByteString.copyFromUtf8("Bitlap")))
        case DbmsVer =>
          Some(GetInfoValue(ByteString.copyFromUtf8(BitlapVersionInfo.getVersion)))
        case _ => None
      }
    }.someOrFail(
      BitlapSQLException("Unrecognized GetInfoType value: " + getInfoType.toString)
    )
}

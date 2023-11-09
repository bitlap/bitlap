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

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ListBuffer

import org.bitlap.common.{ BitlapConf, BitlapVersionInfo }
import org.bitlap.core.catalog.metadata.Database
import org.bitlap.jdbc.BitlapSQLException
import org.bitlap.network.enumeration.*
import org.bitlap.network.enumeration.GetInfoType.*
import org.bitlap.network.handles.*
import org.bitlap.network.models.*

import com.google.protobuf.ByteString
import com.typesafe.scalalogging.StrictLogging

import zio.{ System as _, * }

/** Bitlap session implementation on a single machine
 */
final class SimpleLocalSession(
  val username: String,
  val password: String,
  _sessionConf: Map[String, String],
  val sessionManager: SessionManager,
  val sessionHandle: SessionHandle = SessionHandle(HandleIdentifier()),
  val sessionState: AtomicBoolean = new AtomicBoolean(false),
  val creationTime: Long = System.currentTimeMillis())
    extends Session
    with StrictLogging {

  private[session] var _lastAccessTime: Long = _

  private[session] var _currentSchema: String = _

  override def lastAccessTime: Long = _lastAccessTime

  override def lastAccessTime_=(time: Long): Unit = _lastAccessTime = time

  override def sessionConf: BitlapConf = org.bitlap.core.BitlapContext.bitlapConf.clone(_sessionConf)

  override def currentSchema: String = _currentSchema

  override def currentSchema_=(schema: String): Unit = _currentSchema = schema

  override def open(): Unit = {
    this.sessionState.compareAndSet(false, true)
    _lastAccessTime = System.currentTimeMillis()
    _currentSchema = Database.DEFAULT_DATABASE
  }

  override def executeStatement(
    statement: String,
    confOverlay: Map[String, String]
  ): OperationHandle =
    newExecuteStatementOperation(
      this,
      statement,
      _sessionConf ++ confOverlay
    ).opHandle

  override def executeStatement(
    statement: String,
    confOverlay: Map[String, String],
    queryTimeout: Long
  ): OperationHandle =
    executeStatement(statement, confOverlay)

  override def fetchResults(
    operationHandle: OperationHandle
  ): Task[RowSet] =
    sessionManager.getOperation(operationHandle).map { op =>
      val rows = op.getNextResultSet()
      op.remove(operationHandle) // TODO (work with fetch offset & size)
      rows
    }

  override def getResultSetMetadata(
    operationHandle: OperationHandle
  ): Task[TableSchema] =
    sessionManager.getOperation(operationHandle).map(_.getResultSetSchema())

  override def closeOperation(operationHandle: OperationHandle): Unit =
    this.synchronized {
      val op = SessionManager.OperationStoreMap.getOrDefault(operationHandle, null)
      if op != null then {
        op.setState(OperationState.ClosedState)
        removeOperation(operationHandle)
      }
    }

  override def cancelOperation(operationHandle: OperationHandle): Unit =
    this.synchronized {
      val op = SessionManager.OperationStoreMap.getOrDefault(operationHandle, null)
      if op != null then {
        if op.state.terminal then {
          logger.info(s"$operationHandle Operation is already aborted in state - ${op.state}")
        } else {
          logger.info(s"$operationHandle Attempting to cancel from state - ${op.state}")
          op.setState(OperationState.CanceledState)
          removeOperation(operationHandle)
        }
      }
    }

  override def removeExpiredOperations(handles: List[OperationHandle]): List[Operation] = {
    val removed = new ListBuffer[Operation]()
    for handle <- handles do {
      val operation = removeTimedOutOperation(handle)
      operation.foreach(f => removed.append(f))
    }
    removed.toList
  }

  /** Create an operation for the SQL and execute it. For now, we put the results in memory by Map.
   */
  private def newExecuteStatementOperation(
    parentSession: Session,
    statement: String,
    confOverlay: scala.collection.Map[String, String] = Map.empty
  ): Operation = {
    val operation = new SimpleOperation(
      parentSession,
      OperationType.ExecuteStatement,
      hasResultSet = true
    )
    confOverlay.foreach(kv => operation.confOverlay.put(kv._1, kv._2))
    this.synchronized {
      SessionManager.OperationHandleVector.add(operation.opHandle)
      SessionManager.OperationStoreMap.put(operation.opHandle, operation)
    }
    operation.statement = statement
    operation.run()
    operation
  }

  private def removeOperation(operationHandle: OperationHandle): Option[Operation] =
    this.synchronized {
      val r = SessionManager.OperationStoreMap.remove(operationHandle)
      SessionManager.OperationHandleVector.remove(operationHandle)
      Option(r)
    }

  private def removeTimedOutOperation(operationHandle: OperationHandle): Option[Operation] = {
    val operation = SessionManager.OperationStoreMap.get(operationHandle)
    if operation != null && operation.isTimedOut(System.currentTimeMillis) then {
      return removeOperation(operationHandle)
    }
    Option(operation)
  }

  override def getNoOperationTime: Long = {
    val noMoreOpHandle = SessionManager.OperationHandleVector.isEmpty
    if noMoreOpHandle then System.currentTimeMillis - _lastAccessTime
    else 0
  }

  override def getInfo(getInfoType: GetInfoType): Task[GetInfoValue] =
    ZIO.succeed(
      getInfoType match {
        case ServerName =>
          GetInfoValue(ByteString.copyFromUtf8("Bitlap"))
        case ServerConf =>
          GetInfoValue(ByteString.copyFromUtf8(org.bitlap.core.BitlapContext.bitlapConf.toJson))
        case DbmsName =>
          GetInfoValue(ByteString.copyFromUtf8("Bitlap"))
        case DbmsVer =>
          GetInfoValue(ByteString.copyFromUtf8(BitlapVersionInfo.getVersion))
        case _ =>
          throw BitlapSQLException("Unrecognized GetInfoType value: " + getInfoType.toString)
      }
    )
}

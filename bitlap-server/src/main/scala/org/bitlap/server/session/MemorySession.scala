/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.session

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*

import org.bitlap.common.{ BitlapConf, BitlapVersionInfo }
import org.bitlap.jdbc.BitlapSQLException
import org.bitlap.network.enumeration.*
import org.bitlap.network.enumeration.GetInfoType.*
import org.bitlap.network.handles.*
import org.bitlap.network.models.*

import com.google.protobuf.ByteString

import zio.Task

/** bitlap 单机会话实现
 *  @author
 *    梦境迷离
 *  @version 1.0,2021/12/3
 */
final class MemorySession(
  val username: String,
  val password: String,
  _sessionConf: scala.collection.Map[String, String],
  val sessionManager: SessionManager,
  val sessionHandle: SessionHandle = new SessionHandle(new HandleIdentifier()),
  val sessionState: AtomicBoolean = new AtomicBoolean(false),
  val creationTime: Long = System.currentTimeMillis())
    extends Session {

  private[session] var _lastAccessTime: Long = _

  override def lastAccessTime: Long = _lastAccessTime

  override def sessionConf: BitlapConf = new BitlapConf(_sessionConf.asJava)

  override def open(): Unit = {
    this.sessionState.compareAndSet(false, true)
    _lastAccessTime = System.currentTimeMillis()
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
      op.remove(operationHandle) // TODO: work with fetch offset & size
      rows
    }

  override def getResultSetMetadata(
    operationHandle: OperationHandle
  ): Task[TableSchema] =
    sessionManager.getOperation(operationHandle).map(_.getResultSetSchema())

  override def closeOperation(operationHandle: OperationHandle): Unit =
    this.synchronized {
      val op = SessionManager.operationStore.getOrElse(operationHandle, null)
      if op != null then {
        op.setState(OperationState.ClosedState)
        removeOperation(operationHandle)
      }
    }

  override def cancelOperation(operationHandle: OperationHandle): Unit =
    this.synchronized {
      val op = SessionManager.operationStore.getOrElse(operationHandle, null)
      if op != null then {
        if op.state.terminal then {
          println(s"$operationHandle Operation is already aborted in state - ${op.state}")
        } else {
          println(s"$operationHandle Attempting to cancel from state - ${op.state}")
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
    val operation = new MemoryOperation(
      parentSession,
      OperationType.ExecuteStatement,
      hasResultSet = true
    )
    confOverlay.foreach(kv => operation.confOverlay.put(kv._1, kv._2))
    addOperation(operation)
    operation.statement = statement
    operation.run()
    operation
  }

  private def addOperation(operation: Operation) =
    this.synchronized {
      SessionManager.opHandleSet.append(operation.opHandle)
      SessionManager.operationStore.put(operation.opHandle, operation)
    }

  private def removeOperation(operationHandle: OperationHandle): Option[Operation] =
    this.synchronized {
      val r   = SessionManager.operationStore.remove(operationHandle)
      val idx = SessionManager.opHandleSet.indexOf(operationHandle)
      if idx != -1 then {
        SessionManager.opHandleSet.remove(idx)
      }
      r
    }

  private def removeTimedOutOperation(operationHandle: OperationHandle): Option[Operation] = {
    val operation = SessionManager.operationStore.get(operationHandle)
    if operation != null && operation.get.isTimedOut(System.currentTimeMillis) then {
      return removeOperation(operationHandle)
    }
    operation
  }

  override def getNoOperationTime: Long = {
    val noMoreOpHandle = SessionManager.opHandleSet.isEmpty
    if noMoreOpHandle then System.currentTimeMillis - _lastAccessTime
    else 0
  }

  override def getInfo(getInfoType: GetInfoType): GetInfoValue =
    getInfoType match {
      case ServerName =>
        GetInfoValue(ByteString.copyFromUtf8("Bitlap"))
      case DbmsName =>
        GetInfoValue(ByteString.copyFromUtf8("Bitlap"))
      case DbmsVer =>
        GetInfoValue(ByteString.copyFromUtf8(BitlapVersionInfo.getVersion))
      case _ =>
        throw BitlapSQLException("Unrecognized GetInfoType value: " + getInfoType.toString)
    }
}

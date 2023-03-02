/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.session

import org.bitlap.common.BitlapConf
import org.bitlap.network._
import org.bitlap.network.handles._
import org.bitlap.network.models._

import java.util.concurrent.atomic.AtomicBoolean
import scala.jdk.CollectionConverters._
import scala.collection.mutable.ListBuffer

/** bitlap 会话
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
  val creationTime: Long = System.currentTimeMillis()
) extends Session {

  override var lastAccessTime: Long = _

  override def sessionConf: BitlapConf = new BitlapConf(_sessionConf.asJava)

  override def open(): Unit = {
    this.sessionState.compareAndSet(false, true)
    lastAccessTime = System.currentTimeMillis()
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
  ): RowSet = {
    val op   = sessionManager.getOperation(operationHandle)
    val rows = op.getNextResultSet()
    op.remove(operationHandle) // TODO: work with fetch offset & size
    rows
  }

  override def getResultSetMetadata(
    operationHandle: OperationHandle
  ): TableSchema =
    sessionManager.getOperation(operationHandle).getResultSetSchema()

  override def closeOperation(operationHandle: OperationHandle): Unit =
    this.synchronized {
      val op = sessionManager.operationStore.getOrElse(operationHandle, null)
      if (op != null) {
        op.setState(OperationState.ClosedState)
        removeOperation(operationHandle)
      }
    }

  override def cancelOperation(operationHandle: OperationHandle): Unit =
    this.synchronized {
      val op = sessionManager.operationStore.getOrElse(operationHandle, null)
      if (op != null) {
        if (op.state.terminal) {
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
    for (handle <- handles) {
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

  private def addOperation(operation: Operation) {
    this.synchronized {
      sessionManager.opHandleSet.append(operation.opHandle)
      sessionManager.operationStore.put(operation.opHandle, operation)
    }
  }

  private def removeOperation(operationHandle: OperationHandle): Option[Operation] =
    this.synchronized {
      val r   = sessionManager.operationStore.remove(operationHandle)
      val idx = sessionManager.opHandleSet.indexOf(operationHandle)
      if (idx != -1) {
        sessionManager.opHandleSet.remove(idx)
      }
      r
    }

  private def removeTimedOutOperation(operationHandle: OperationHandle): Option[Operation] = {
    val operation = sessionManager.operationStore.get(operationHandle)
    if (operation != null && operation.get.isTimedOut(System.currentTimeMillis)) {
      return removeOperation(operationHandle)
    }
    operation
  }

  override def getNoOperationTime: Long = {
    val noMoreOpHandle = sessionManager.opHandleSet.isEmpty
    if (noMoreOpHandle) System.currentTimeMillis - lastAccessTime
    else 0
  }
}

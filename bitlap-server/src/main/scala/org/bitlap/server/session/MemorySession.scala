/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.session

import org.bitlap.common.BitlapConf
import org.bitlap.network._
import org.bitlap.network.handles._
import org.bitlap.network.models._

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

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

  private val operationStore: mutable.HashMap[OperationHandle, Operation] =
    mutable.HashMap[OperationHandle, Operation]()

  override var lastAccessTime: Long = _

  private lazy val opHandleSet = ListBuffer[OperationHandle]()

  override def sessionConf: BitlapConf = new BitlapConf(_sessionConf.asJava)

  override def open(): Unit = {
    this.sessionState.compareAndSet(false, true)
    lastAccessTime = System.currentTimeMillis()
  }

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    confOverlay: Map[String, String]
  ): OperationHandle = {
    val operation = newExecuteStatementOperation(
      this,
      statement,
      _sessionConf ++ confOverlay
    )
    opHandleSet.append(operation.opHandle)
    operation.opHandle
  }

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    confOverlay: Map[String, String],
    queryTimeout: Long
  ): OperationHandle =
    executeStatement(sessionHandle, statement, confOverlay)

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

  override def close(operationHandle: OperationHandle): Unit =
    this.synchronized {
      val closedOps = new ListBuffer[OperationHandle]()
      for (opHandle <- opHandleSet) {
        val op = operationStore.getOrElse(operationHandle, null)
        if (op != null) {
          op.setState(OperationState.ClosedState)
          operationStore.remove(operationHandle)
        }
        closedOps.append(opHandle)
      }
      closedOps.zipWithIndex.foreach { case (_, i) =>
        opHandleSet.remove(i)
      }

      sessionState.set(false)
    }

  override def cancel(operationHandle: OperationHandle): Unit =
    this.synchronized {
      val op = operationStore.getOrElse(operationHandle, null)
      if (op == null) {
        true
      } else {
        op.setState(OperationState.CanceledState)
        operationStore.remove(operationHandle)
        true
      }
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
    operation.setStatement(statement)
    operation.run()
    operation
  }

  private def addOperation(operation: Operation) {
    this.synchronized {
      operationStore.put(operation.opHandle, operation)
    }
  }
}

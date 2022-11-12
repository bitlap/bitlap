/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.session

import org.bitlap.jdbc.BitlapSQLException
import org.bitlap.network.OperationType
import org.bitlap.network.handles.OperationHandle

import scala.collection.mutable
import org.bitlap.network.OperationState

/** bitlap 操作管理器
 *  @author
 *    梦境迷离
 *  @since 2021/11/20
 *  @version 1.0
 */
class OperationManager {

  private val operationStore: mutable.HashMap[OperationHandle, Operation] =
    mutable.HashMap[OperationHandle, Operation]()

  /** Create an operation for the SQL and execute it. For now, we put the results in memory by Map.
   */
  def newExecuteStatementOperation(
    parentSession: Session,
    statement: String,
    confOverlay: Map[String, String] = Map.empty
  ): Operation = {
    val operation = new MemoryOperation(
      parentSession,
      OperationType.ExecuteStatement,
      hasResultSet = true
    )
    confOverlay.foreach(kv => operation.confOverlay.put(kv._1, kv._2))
    operation.setStatement(statement)
    operation.run()
    addOperation(operation)
    operation.setState(OperationState.FinishedState)
    operation
  }

  def addOperation(operation: Operation) {
    this.synchronized {
      operationStore.put(operation.opHandle, operation)
    }
  }

  def getOperation(operationHandle: OperationHandle): Operation =
    this.synchronized {
      val op = operationStore.getOrElse(operationHandle, null)
      if (op == null) {
        throw BitlapSQLException(s"Invalid OperationHandle: $operationHandle")
      } else {
        op.getState match {
          case OperationState.FinishedState => op
          case _ =>
            throw BitlapSQLException(s"Invalid OperationState: ${op.getState}")
        }
      }
    }

  def cancelOperation(operationHandle: OperationHandle): Boolean =
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

  def closeOperation(operationHandle: OperationHandle): Boolean =
    this.synchronized {
      val op = operationStore.getOrElse(operationHandle, null)
      if (op == null) {
        true
      } else {
        op.setState(OperationState.ClosedState)
        operationStore.remove(operationHandle)
        true
      }
    }
}

/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc

import org.bitlap.jdbc.BSQLException
import org.bitlap.network.OperationType
import org.bitlap.network.handles.OperationHandle

import scala.collection.mutable

/** @author
 *    梦境迷离
 *  @since 2021/11/20
 *  @version 1.0
 */
class OperationManager {

  private val handleToOperation: mutable.HashMap[OperationHandle, Operation] =
    mutable.HashMap[OperationHandle, Operation]()

  /** Create an operation for the SQL and execute it. For now, we put the results in memory by Map.
   */
  def newExecuteStatementOperation(
    parentSession: Session,
    statement: String,
    confOverlay: Map[String, String] = Map.empty
  ): Operation = {
    val operation = new BitlapOperation(
      parentSession,
      OperationType.EXECUTE_STATEMENT,
      hasResultSet = true
    )
    confOverlay.foreach(kv => operation.confOverlay.put(kv._1, kv._2))
    operation.setStatement(statement)
    operation.run()
    addOperation(operation)
    operation
  }

  def addOperation(operation: Operation) {
    this.synchronized {
      handleToOperation.put(operation.opHandle, operation)
    }
  }

  def getOperation(operationHandle: OperationHandle): Operation =
    this.synchronized {
      val op = handleToOperation.getOrElse(operationHandle, null)
      if (op == null) {
        throw BSQLException("Invalid OperationHandle: $operationHandle")
      } else {
        op
      }
    }
}

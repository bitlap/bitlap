package org.bitlap.net.operation

import cn.hutool.core.util.ServiceLoaderUtil
import org.bitlap.net.BSQLException
import org.bitlap.net.handles.OperationHandle
import org.bitlap.net.operation.operations.Operation
import org.bitlap.net.session.Session

import scala.collection.mutable

/**
 *
 * @author 梦境迷离
 * @since 2021/11/20
 * @version 1.0
 */
class OperationManager {

  private val operationFactory: OperationFactory = ServiceLoaderUtil.loadFirst(classOf[OperationFactory])
  private val handleToOperation: mutable.HashMap[OperationHandle, Operation] = mutable.HashMap[OperationHandle, Operation]()

  /**
   * Create an operation for the SQL and execute it. For now, we put the results in memory by Map.
   */
  def newExecuteStatementOperation(parentSession: Session, statement: String, confOverlay: Map[String, String] = Map.empty): Operation = {
    val operation = operationFactory.create(parentSession, OperationType.EXECUTE_STATEMENT, hasResultSet = true)
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

  def getOperation(operationHandle: OperationHandle): Operation = {
    this.synchronized {
      val op = handleToOperation.getOrElse(operationHandle, null)
      if (op == null) {
        throw BSQLException("Invalid OperationHandle: $operationHandle")
      } else {
        op
      }
    }
  }
}

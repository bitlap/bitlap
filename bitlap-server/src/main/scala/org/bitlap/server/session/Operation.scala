/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.session

import com.typesafe.scalalogging.LazyLogging
import org.bitlap.network.handles.OperationHandle
import org.bitlap.network.models._
import org.bitlap.network.OperationType

import scala.collection.mutable
import org.bitlap.network.OperationState

/** bitlap 操作
 *  @author
 *    梦境迷离
 *  @since 2021/11/20
 *  @version 1.0
 */
abstract class Operation(
  val parentSession: Session,
  val opType: OperationType,
  val hasResultSet: Boolean = false
) extends LazyLogging {

  @volatile
  private var state: OperationState = OperationState.InitializedState

  private var statement: String = _

  // super不能用于字段
  def getStatement: String = statement

  def setStatement(statement: String): Unit =
    this.statement = statement

  def getOpHandle: OperationHandle = opHandle

  lazy val opHandle: OperationHandle =
    new OperationHandle(opType, hasResultSet)
  lazy val confOverlay: mutable.Map[String, String] =
    mutable.HashMap[String, String]()

  protected lazy val cache: mutable.HashMap[OperationHandle, QueryResult] =
    mutable.HashMap()

  def run()

  def remove(operationHandle: OperationHandle) {
    cache.remove(operationHandle)
  }

  def getNextResultSet(): RowSet =
    cache.get(opHandle).map(_.rows).getOrElse(RowSet())

  def getResultSetSchema(): TableSchema =
    cache.get(opHandle).map(_.tableSchema).getOrElse(TableSchema())

  def setState(operationState: OperationState): Unit =
    state = operationState

  def getState = state
}

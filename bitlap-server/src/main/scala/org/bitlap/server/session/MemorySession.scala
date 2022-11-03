/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.session

import org.bitlap.common.BitlapConf
import org.bitlap.network.handles._
import org.bitlap.network.models._

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

/** bitlap 会话
 *  @author
 *    梦境迷离
 *  @version 1.0,2021/12/3
 */
class MemorySession(
  val username: String,
  val password: String,
  _sessionConf: Map[String, String],
  val sessionManager: SessionManager,
  val sessionHandle: SessionHandle = new SessionHandle(new HandleIdentifier()),
  val sessionState: AtomicBoolean = new AtomicBoolean(false),
  val creationTime: Long = System.currentTimeMillis()
) extends Session {

  this.sessionState.compareAndSet(false, true)

  override var lastAccessTime: Long               = System.currentTimeMillis()
  override var operationManager: OperationManager = _

  private val opHandleSet = ListBuffer[OperationHandle]()

  override def sessionConf: BitlapConf = new BitlapConf(_sessionConf.asJava)

  override def open(
    sessionConfMap: Map[String, String]
  ): SessionHandle = ???

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    confOverlay: Map[String, String]
  ): OperationHandle = {
    val operation = operationManager.newExecuteStatementOperation(
      this,
      statement,
      confOverlay
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
    val op   = operationManager.getOperation(operationHandle)
    val rows = op.getNextResultSet()
    op.remove(operationHandle) // TODO: work with fetch offset & size
    rows
  }

  override def getResultSetMetadata(
    operationHandle: OperationHandle
  ): TableSchema =
    operationManager.getOperation(operationHandle).getResultSetSchema()

  override def close(): Unit = ???
}

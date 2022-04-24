/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc

import org.bitlap.common.BitlapConf
import org.bitlap.network.{ handles, models }

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

/**
 * @author 梦境迷离
 * @version 1.0,2021/12/3
 */
class BitlapSession extends Session {

  override val sessionState: AtomicBoolean = new AtomicBoolean(false)
  override var sessionHandle: handles.SessionHandle = _
  override var password: String = _
  override var username: String = _
  override val creationTime: Long = System.currentTimeMillis()
  override var sessionConf: BitlapConf = _
  override var sessionManager: SessionManager = _
  override var lastAccessTime: Long = System.currentTimeMillis()
  override var operationManager: OperationManager = _

  private val opHandleSet = ListBuffer[handles.OperationHandle]()

  def this(
    username: String,
    password: String,
    sessionConf: Map[String, String],
    sessionManager: SessionManager,
    sessionHandle: handles.SessionHandle = new handles.SessionHandle(new handles.HandleIdentifier())
  ) = {
    this()
    this.username = username
    this.sessionHandle = sessionHandle
    this.password = password
    this.sessionConf = new BitlapConf(sessionConf.asJava)
    this.sessionState.compareAndSet(false, true)
    this.sessionManager = sessionManager
  }

  override def open(
    sessionConfMap: Map[String, String]
  ): handles.SessionHandle = ???

  override def executeStatement(
    sessionHandle: handles.SessionHandle,
    statement: String,
    confOverlay: Map[String, String]
  ): handles.OperationHandle = {
    val operation = operationManager.newExecuteStatementOperation(
      this,
      statement,
      confOverlay
    )
    opHandleSet.append(operation.opHandle)
    operation.opHandle
  }

  override def executeStatement(
    sessionHandle: handles.SessionHandle,
    statement: String,
    confOverlay: Map[String, String],
    queryTimeout: Long
  ): handles.OperationHandle =
    executeStatement(sessionHandle, statement, confOverlay)

  override def fetchResults(
    operationHandle: handles.OperationHandle
  ): models.RowSet = {
    val op = operationManager.getOperation(operationHandle)
    val rows = op.getNextResultSet()
    op.remove(operationHandle) // TODO: work with fetch offset & size
    rows
  }

  override def getResultSetMetadata(
    operationHandle: handles.OperationHandle
  ): models.TableSchema =
    operationManager.getOperation(operationHandle).getResultSetSchema()

  override def close(): Unit = ???
}

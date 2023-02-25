/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.session

import org.bitlap.common.BitlapConf
import org.bitlap.network.handles._
import org.bitlap.network.models._

import java.util.concurrent.atomic.AtomicBoolean

/** bitlap 会话
 *
 *  @author
 *    梦境迷离
 *  @since 2021/6/6
 *  @version 1.0
 */
trait Session {

  val sessionHandle: SessionHandle
  val password: String
  val username: String
  val sessionManager: SessionManager

  var lastAccessTime: Long

  def sessionConf: BitlapConf

  def sessionState: AtomicBoolean

  def creationTime: Long

  def open(): Unit

  def executeStatement(
    statement: String,
    confOverlay: Map[String, String]
  ): OperationHandle

  def executeStatement(
    statement: String,
    confOverlay: Map[String, String] = Map.empty,
    queryTimeout: Long
  ): OperationHandle

  def fetchResults(operationHandle: OperationHandle): RowSet

  def getResultSetMetadata(operationHandle: OperationHandle): TableSchema

  def closeOperation(operationHandle: OperationHandle)

  def cancelOperation(operationHandle: OperationHandle)

  def removeExpiredOperations(handles: List[OperationHandle]): List[Operation]

  def getNoOperationTime: Long
}

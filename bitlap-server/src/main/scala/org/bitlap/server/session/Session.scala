/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.session

import org.bitlap.common.BitlapConf
import org.bitlap.network.enumeration.GetInfoType
import org.bitlap.network.handles.*
import org.bitlap.network.models.*
import zio.Task

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

  def lastAccessTime: Long

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

  def fetchResults(operationHandle: OperationHandle): Task[RowSet]

  def getResultSetMetadata(operationHandle: OperationHandle): Task[TableSchema]

  def closeOperation(operationHandle: OperationHandle): Unit

  def cancelOperation(operationHandle: OperationHandle): Unit

  def removeExpiredOperations(handles: List[OperationHandle]): List[Operation]

  def getNoOperationTime: Long

  def getInfo(getInfoType: GetInfoType): GetInfoValue
}

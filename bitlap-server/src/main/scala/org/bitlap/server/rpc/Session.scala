/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc

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

  var sessionHandle: SessionHandle
  var password: String
  var username: String
  var sessionConf: BitlapConf
  var sessionManager: SessionManager
  var lastAccessTime: Long
  var operationManager: OperationManager

  def sessionState: AtomicBoolean

  def creationTime: Long

  /** open Session
   *
   *  @param sessionConfMap
   *  @return
   *    SessionHandle The Session handle
   */
  def open(sessionConfMap: Map[String, String] = Map.empty): SessionHandle

  /** execute statement
   *
   *  @param statement
   *  @param confOverlay
   *  @return
   *    OperationHandle The Operate handle
   */
  def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    confOverlay: Map[String, String]
  ): OperationHandle

  /** execute statement
   *
   *  @param statement
   *  @param confOverlay
   *  @param queryTimeout
   *  @return
   *    OperationHandle The Operate handle
   */
  def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    confOverlay: Map[String, String] = Map.empty,
    queryTimeout: Long
  ): OperationHandle

  def fetchResults(operationHandle: OperationHandle): RowSet

  def getResultSetMetadata(operationHandle: OperationHandle): TableSchema

  // TODO: close OperationHandle

  /** close Session
   */
  def close()
}

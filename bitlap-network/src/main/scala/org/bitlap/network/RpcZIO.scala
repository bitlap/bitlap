/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import org.bitlap.network.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.models.{ FetchResults, TableSchema }
import zio.Task

/** @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
trait RpcZIO extends Rpc[Task] {

  def openSession(
    username: String,
    password: String,
    configuration: Map[String, String]
  ): Task[SessionHandle]

  def closeSession(sessionHandle: SessionHandle): Task[Unit]

  def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String]
  ): Task[OperationHandle]

  def fetchResults(opHandle: OperationHandle): Task[FetchResults]

  def getResultSetMetadata(opHandle: OperationHandle): Task[TableSchema]

  def getColumns(
    sessionHandle: SessionHandle,
    tableName: String = null,
    schemaName: String = null,
    columnName: String = null
  ): Task[OperationHandle]

  /** get databases or schemas from catalog
   *
   *  @see
   *    `show databases`
   */
  def getDatabases(pattern: String): Task[OperationHandle]

  /** get tables from catalog with database name
   *
   *  @see
   *    `show tables in [db_name]`
   */
  def getTables(database: String, pattern: String): Task[OperationHandle]

  /** get schemas from catalog
   *
   *  @see
   *    `show tables in [db_name]`
   */
  def getSchemas(
    sessionHandle: SessionHandle,
    catalogName: String,
    schemaName: String
  ): Task[OperationHandle]

}

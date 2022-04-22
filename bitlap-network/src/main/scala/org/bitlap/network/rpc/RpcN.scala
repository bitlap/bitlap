/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network.rpc

import org.bitlap.network.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.models.{ RowSet, TableSchema }

/**
 * @author 梦境迷离
 * @version 1.0,2022/4/21
 */
trait RpcN[F[_, _, _]] {

  def openSession(
    username: String,
    password: String,
    configuration: Map[String, String] = Map.empty
  ): F[Any, Throwable, SessionHandle]

  def closeSession(sessionHandle: SessionHandle): F[Any, Throwable, Unit]

  def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    confOverlay: Map[String, String]
  ): F[Any, Throwable, OperationHandle]

  def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String] = Map.empty
  ): F[Any, Throwable, OperationHandle]

  def fetchResults(opHandle: OperationHandle): F[Any, Throwable, RowSet]

  def getResultSetMetadata(opHandle: OperationHandle): F[Any, Throwable, TableSchema]

  def getColumns(
    sessionHandle: SessionHandle,
    tableName: String = null,
    schemaName: String = null,
    columnName: String = null
  ): F[Any, Throwable, OperationHandle]

  /**
   * get databases or schemas from catalog
   *
   * @see `show databases`
   */
  def getDatabases(pattern: String): F[Any, Throwable, List[String]]

  /**
   * get tables from catalog with database name
   *
   * @see `show tables in [db_name]`
   */
  def getTables(database: String, pattern: String): F[Any, Throwable, List[String]]

}

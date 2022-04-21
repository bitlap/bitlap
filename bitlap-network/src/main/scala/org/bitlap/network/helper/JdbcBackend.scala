/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network.helper

import org.bitlap.network.NetworkHelper
import org.bitlap.network.types.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.types.models.{ RowSet, TableSchema }

/**
 * network helper for jdbc
 */
trait JdbcBackend[F[_]] extends NetworkHelper[F] {

  def openSession(
    username: String,
    password: String,
    configuration: Map[String, String] = Map.empty
  ): F[SessionHandle]

  def closeSession(sessionHandle: SessionHandle): F[Unit]

  def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    confOverlay: Map[String, String]
  ): F[OperationHandle]

  def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String] = Map.empty
  ): F[OperationHandle]

  def fetchResults(opHandle: OperationHandle): F[RowSet]

  def getResultSetMetadata(opHandle: OperationHandle): F[TableSchema]

  def getColumns(
    sessionHandle: SessionHandle,
    tableName: String = null,
    schemaName: String = null,
    columnName: String = null
  ): F[OperationHandle]
}

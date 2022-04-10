package org.bitlap.network.helper

import org.bitlap.network.types.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.types.models.{ RowSet, TableSchema }


/**
 * network helper for jdbc
 */
trait JdbcHelper extends CommonHelper {

  def openSession(
    username: String,
    password: String,
    configuration: Map[String, String] = Map.empty
  ): SessionHandle

  def closeSession(sessionHandle: SessionHandle): Unit

  def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    confOverlay: Map[String, String]
  ): OperationHandle

  def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String] = Map.empty
  ): OperationHandle

  def fetchResults(opHandle: OperationHandle): RowSet

  def getResultSetMetadata(opHandle: OperationHandle): TableSchema

  def getColumns(
    sessionHandle: SessionHandle,
    tableName: String = null,
    schemaName: String = null,
    columnName: String = null
  ): OperationHandle
}

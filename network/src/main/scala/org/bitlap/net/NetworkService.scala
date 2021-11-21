package org.bitlap.net

import org.bitlap.net.handles.{ OperationHandle, SessionHandle }
import org.bitlap.net.models.{ RowSet, TableSchema }

/**
 *
 * @author 梦境迷离
 * @since 2021/11/20
 * @version 1.0
 */
trait NetworkService {

  def openSession(username: String, password: String, configuration: Map[String, String] = Map.empty): SessionHandle

  def closeSession(sessionHandle: SessionHandle)

  def executeStatement(sessionHandle: SessionHandle, statement: String, confOverlay: Map[String, String]): OperationHandle

  def executeStatement(sessionHandle: SessionHandle, statement: String, queryTimeout: Long, confOverlay: Map[String, String] = Map.empty): OperationHandle

  def fetchResults(opHandle: OperationHandle): RowSet

  def getResultSetMetadata(opHandle: OperationHandle): TableSchema

  def getColumns(sessionHandle: SessionHandle, tableName: String = null, schemaName: String = null, columnName: String = null): OperationHandle

  def getTables(sessionHandle: SessionHandle, tableName: String = null, schemaName: String = null): OperationHandle

  def getSchemas(sessionHandle: SessionHandle): OperationHandle
}

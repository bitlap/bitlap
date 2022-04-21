/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc.backend

import org.bitlap.network.Identity
import org.bitlap.network.helper.JdbcBackend
import org.bitlap.network.types.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.types.models.{ RowSet, TableSchema }
import org.bitlap.server.rpc.SessionManager
import org.bitlap.network.types.OperationType

/**
 * @author 梦境迷离
 * @since 2021/11/20
 * @version 1.0
 */
class JdbcSyncBackend extends JdbcBackend[Identity] {

  private val sessionManager = new SessionManager()
  sessionManager.startListener()

  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String] = Map.empty
  ): Identity[SessionHandle] = {
    val session = sessionManager.openSession(username, password, configuration)
    session.sessionHandle
  }

  override def closeSession(sessionHandle: SessionHandle): Identity[Unit] = sessionManager.closeSession(sessionHandle)

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    confOverlay: Map[String, String]
  ): Identity[OperationHandle] = {
    val session = sessionManager.getSession(sessionHandle)
    sessionManager.refreshSession(sessionHandle, session)
    session.executeStatement(sessionHandle, statement, confOverlay)
  }

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String] = Map.empty
  ): Identity[OperationHandle] = {
    val session = sessionManager.getSession(sessionHandle)
    sessionManager.refreshSession(sessionHandle, session)
    session.executeStatement(
      sessionHandle,
      statement,
      confOverlay,
      queryTimeout
    )
  }

  override def fetchResults(opHandle: OperationHandle): Identity[RowSet] = {
    val operation = sessionManager.operationManager.getOperation(opHandle)
    val session = operation.parentSession
    sessionManager.refreshSession(session.sessionHandle, session)
    session.fetchResults(opHandle)
  }

  override def getResultSetMetadata(opHandle: OperationHandle): Identity[TableSchema] = {
    val operation = sessionManager.operationManager.getOperation(opHandle)
    val session = operation.parentSession
    sessionManager.refreshSession(session.sessionHandle, session)
    session.getResultSetMetadata(opHandle)
  }

  override def getColumns(
    sessionHandle: SessionHandle,
    tableName: String = null,
    schemaName: String = null,
    columnName: String = null
  ): Identity[OperationHandle] = new OperationHandle(OperationType.GET_COLUMNS)

  /**
   * get databases or schemas from catalog
   *
   * @see `show databases`
   */
  override def getDatabases(pattern: String): Identity[List[String]] = ???

  /**
   * get tables from catalog with database name
   *
   * @see `show tables in [db_name]`
   */
  override def getTables(database: String, pattern: String): Identity[List[String]] = ???
}

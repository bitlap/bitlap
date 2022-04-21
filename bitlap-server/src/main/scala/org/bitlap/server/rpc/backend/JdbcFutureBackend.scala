/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc.backend

import org.bitlap.network.helper.JdbcBackend
import org.bitlap.network.types.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.types.models
import org.bitlap.network.types.models.RowSet

import scala.concurrent.Future

/**
 * @author 梦境迷离
 * @version 1.0,2022/4/21
 */
class JdbcFutureBackend extends JdbcBackend[Future] {

  // TODO
  private lazy val delegateBackend = new JdbcSyncBackend()

  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String]
  ): Future[SessionHandle] = Future.successful(delegateBackend.openSession(username, password, configuration))

  override def closeSession(sessionHandle: SessionHandle): Future[Unit] =
    Future.successful(delegateBackend.closeSession(sessionHandle))

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    confOverlay: Map[String, String]
  ): Future[OperationHandle] =
    Future.successful(delegateBackend.executeStatement(sessionHandle, statement, confOverlay))

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String]
  ): Future[OperationHandle] =
    Future.successful(delegateBackend.executeStatement(sessionHandle, statement, queryTimeout, confOverlay))

  override def fetchResults(opHandle: OperationHandle): Future[RowSet] =
    Future.successful(delegateBackend.fetchResults(opHandle))

  override def getResultSetMetadata(opHandle: OperationHandle): Future[models.TableSchema] =
    Future.successful(delegateBackend.getResultSetMetadata(opHandle))

  override def getColumns(
    sessionHandle: SessionHandle,
    tableName: String,
    schemaName: String,
    columnName: String
  ): Future[OperationHandle] =
    Future.successful(delegateBackend.getColumns(sessionHandle, tableName, schemaName, columnName))

  override def getDatabases(pattern: String): Future[List[String]] =
    Future.successful(delegateBackend.getDatabases(pattern))

  override def getTables(database: String, pattern: String): Future[List[String]] =
    Future.successful(delegateBackend.getTables(database, pattern))
}

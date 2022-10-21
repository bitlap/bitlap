/* Copyright (c) 2022 bitlap.org */
package org.bitlap.client

import org.bitlap.network._
import org.bitlap.network.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.models._

/** This class mainly wraps zio asynchronous calls.
 *
 *  @author
 *    梦境迷离
 *  @since 2021/11/21
 *  @version 1.0
 */
class BitlapSyncClient(uri: String, port: Int, props: Map[String, String]) extends RpcIdentity with RpcStatus {

  private lazy val delegateClient = new BitlapZioClient(uri, port, props)

  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String]
  ): Identity[SessionHandle] = delegateClient.sync {
    delegateClient.openSession(username, password, configuration)
  }

  override def closeSession(sessionHandle: SessionHandle): Identity[Unit] = delegateClient.sync {
    delegateClient.closeSession(sessionHandle)
  }

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String] = Map.empty
  ): Identity[OperationHandle] = delegateClient.sync {
    delegateClient.executeStatement(sessionHandle, statement, queryTimeout, confOverlay)
  }

  override def fetchResults(opHandle: OperationHandle, maxRows: Int, fetchType: Int): Identity[FetchResults] =
    delegateClient.sync {
      delegateClient.fetchResults(opHandle, maxRows, fetchType)
    }

  override def getResultSetMetadata(opHandle: OperationHandle): Identity[TableSchema] = delegateClient.sync {
    delegateClient.getResultSetMetadata(opHandle)
  }

  override def getColumns(
    sessionHandle: SessionHandle,
    schemaName: String,
    tableName: String,
    columnName: String
  ): Identity[OperationHandle] = delegateClient.sync {
    delegateClient.getColumns(sessionHandle, tableName, schemaName, columnName)
  }

  override def getDatabases(pattern: String): Identity[OperationHandle] = delegateClient.sync {
    delegateClient.getDatabases(pattern)
  }
  override def getTables(database: String, pattern: String): Identity[OperationHandle] = delegateClient.sync {
    delegateClient.getTables(database, pattern)
  }

  override def getSchemas(
    sessionHandle: SessionHandle,
    catalogName: String,
    schemaName: String
  ): Identity[OperationHandle] = delegateClient.sync {
    delegateClient.getSchemas(sessionHandle, catalogName, schemaName)
  }
}

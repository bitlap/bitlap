/* Copyright (c) 2022 bitlap.org */
package org.bitlap.client

import org.bitlap.network.RpcStatus
import org.bitlap.network.dsl.blocking
import org.bitlap.network.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.models._
import org.bitlap.network.rpc.{ Identity, RpcF }

/**
 * This class mainly wraps the RPC call procedure used inside JDBC.
 *
 * @author 梦境迷离
 * @since 2021/11/21
 * @version 1.0
 */
private[bitlap] class BitlapSyncClient(uri: String, port: Int, props: Map[String, String])
    extends RpcF[Identity]
    with RpcStatus {

  private lazy val delegateClient = new BitlapZioClient(uri, port, props)

  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String]
  ): Identity[SessionHandle] = blocking {
    delegateClient.openSession(username, password, configuration)
  }

  override def closeSession(sessionHandle: SessionHandle): Identity[Unit] = blocking {
    delegateClient.closeSession(sessionHandle)
  }

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long = delegateClient.readTimeout,
    confOverlay: Map[String, String] = Map.empty
  ): Identity[OperationHandle] = blocking {
    delegateClient.executeStatement(sessionHandle, statement, queryTimeout, confOverlay)
  }

  override def fetchResults(opHandle: OperationHandle): Identity[FetchResults] = blocking {
    delegateClient.fetchResults(opHandle)
  }

  override def getResultSetMetadata(opHandle: OperationHandle): Identity[TableSchema] = blocking {
    delegateClient.getResultSetMetadata(opHandle)
  }

  override def getColumns(
    sessionHandle: SessionHandle,
    schemaName: String,
    tableName: String,
    columnName: String
  ): Identity[OperationHandle] = blocking {
    delegateClient.getColumns(sessionHandle, tableName, schemaName, columnName)
  }

  override def getDatabases(pattern: String): Identity[OperationHandle] = blocking {
    delegateClient.getDatabases(pattern)
  }
  override def getTables(database: String, pattern: String): Identity[OperationHandle] = blocking {
    delegateClient.getTables(database, pattern)
  }

  override def getSchemas(
    sessionHandle: SessionHandle,
    catalogName: String,
    schemaName: String
  ): Identity[OperationHandle] = blocking {
    delegateClient.getSchemas(sessionHandle, catalogName, schemaName)
  }
}

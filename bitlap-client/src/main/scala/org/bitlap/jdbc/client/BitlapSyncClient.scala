/* Copyright (c) 2022 bitlap.org */
package org.bitlap.jdbc.client

import org.bitlap.common.BitlapConf
import org.bitlap.network.dsl.blocking
import org.bitlap.network.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.rpc.{ Identity, RpcF }
import org.bitlap.network.{ models, RpcStatus }

import scala.jdk.CollectionConverters._

/**
 * This class mainly wraps the RPC call procedure used inside JDBC.
 *
 * @author 梦境迷离
 * @since 2021/11/21
 * @version 1.0
 */
private[jdbc] class BitlapSyncClient(uri: String, port: Int, props: Map[String, String])
    extends RpcF[Identity]
    with RpcStatus {
  private lazy val conf: BitlapConf = new BitlapConf(props.asJava)
  private val readTimeout: java.lang.Long = conf.get(BitlapConf.NODE_READ_TIMEOUT)

  private lazy val delegateClient = new BitlapZioClient(uri, port)

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
    confOverlay: Map[String, String]
  ): Identity[OperationHandle] = blocking {
    delegateClient.executeStatement(sessionHandle, statement, confOverlay)
  }

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long = readTimeout,
    confOverlay: Map[String, String] = Map.empty
  ): Identity[OperationHandle] = blocking {
    delegateClient.executeStatement(sessionHandle, statement, queryTimeout, confOverlay)
  }

  override def fetchResults(opHandle: OperationHandle): Identity[models.RowSet] = blocking {
    delegateClient.fetchResults(opHandle)
  }

  override def getResultSetMetadata(opHandle: OperationHandle): Identity[models.TableSchema] = blocking {
    delegateClient.getResultSetMetadata(opHandle)
  }

  override def getColumns(
    sessionHandle: SessionHandle,
    tableName: String,
    schemaName: String,
    columnName: String
  ): Identity[OperationHandle] = blocking {
    delegateClient.getColumns(sessionHandle, tableName, schemaName, columnName)
  }

  override def getDatabases(pattern: String): Identity[List[String]] = blocking {
    delegateClient.getDatabases(pattern)
  }
  override def getTables(database: String, pattern: String): Identity[List[String]] = blocking {
    delegateClient.getTables(database, pattern)
  }
}

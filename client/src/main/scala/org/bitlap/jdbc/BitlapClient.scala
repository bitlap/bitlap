/* Copyright (c) 2022 bitlap.org */
package org.bitlap.jdbc

import com.alipay.sofa.jraft.rpc.InvokeCallback
import org.bitlap.common.BitlapConf
import org.bitlap.network.proto.driver._
import org.bitlap.network.{ NetworkHelper, RPC }

import java.lang.{ Long => JLong }
import scala.jdk.CollectionConverters._

/**
 * This class mainly wraps the RPC call procedure used inside JDBC.
 *
 * @author 梦境迷离
 * @since 2021/11/21
 * @version 1.0
 */
class BitlapClient(uri: String, props: Map[String, String]) extends NetworkHelper {

  private val conf: BitlapConf = new BitlapConf(props.asJava)
  private val rpcClient = RPC.newClient(uri, conf)
  private val readTimeout: JLong = conf.get(BitlapConf.NODE_READ_TIMEOUT)

  /**
   * Used to open a session during JDBC connection initialization.
   */
  def openSession(): BSessionHandle = {
    val resp = this.rpcClient.invokeSync[BOpenSession.BOpenSessionResp](
      BOpenSession.BOpenSessionReq
        .newBuilder()
        .setUsername(props.getOrElse("user", ""))
        .setPassword(props.getOrElse("password", ""))
        .build()
    )
    verifySuccess(resp.getStatus)
    resp.getSessionHandle

    // TODO: Add heartbeat
  }

  /**
   * Used to close the session when the JDBC connection is closed.
   */
  def closeSession(sessionHandle: BSessionHandle): Unit =
    this.rpcClient.invokeAsync(
      BCloseSession.BCloseSessionReq
        .newBuilder()
        .setSessionHandle(sessionHandle)
        .build(),
      new InvokeCallback() {
        override def complete(o: Any, throwable: Throwable): Unit = ()
      }
    )

  /**
   * Used to execute normal SQL by JDBC. Does not contain `?` placeholders.
   */
  def executeStatement(sessionHandle: BSessionHandle, statement: String): BOperationHandle = {
    val resp = this.rpcClient.invokeSync[BExecuteStatement.BExecuteStatementResp](
      BExecuteStatement.BExecuteStatementReq
        .newBuilder()
        .setSessionHandle(sessionHandle)
        .setStatement(statement)
        .build(),
      readTimeout
    )
    verifySuccess(resp.getStatus)
    resp.getOperationHandle
  }

  /**
   * Used for JDBC to get result set of the specified operation.
   */
  def fetchResults(operationHandle: BOperationHandle): BFetchResults.BFetchResultsResp = {
    val resp = this.rpcClient.invokeSync[BFetchResults.BFetchResultsResp](
      BFetchResults.BFetchResultsReq
        .newBuilder()
        .setOperationHandle(operationHandle)
        .build()
    )
    verifySuccess(resp.getStatus)
    resp
  }

  def getSchemas(
    sessionHandle: BSessionHandle,
    catalogName: String = null,
    schemaName: String = null
  ): BOperationHandle = {
    val resp = this.rpcClient.invokeSync[BGetSchemas.BGetSchemasResp](
      BGetSchemas.BGetSchemasReq
        .newBuilder()
        .setSessionHandle(sessionHandle)
        .setCatalogName(catalogName)
        .setSchemaName(schemaName)
        .build()
    )
    verifySuccess(resp.getStatus)
    resp.getOperationHandle
  }

  def getTables(
    sessionHandle: BSessionHandle,
    tableName: String = null,
    schemaName: String = null
  ): BOperationHandle = {
    val resp = this.rpcClient.invokeSync[BGetTables.BGetTablesResp](
      BGetTables.BGetTablesReq
        .newBuilder()
        .setSessionHandle(sessionHandle)
        .setTableName(tableName)
        .setSchemaName(schemaName)
        .build()
    )
    verifySuccess(resp.getStatus)
    resp.getOperationHandle
  }

  def getColumns(
    sessionHandle: BSessionHandle,
    tableName: String = null,
    schemaName: String = null,
    columnName: String = null
  ): BOperationHandle = {
    val resp = this.rpcClient.invokeSync[BGetColumns.BGetColumnsResp](
      BGetColumns.BGetColumnsReq
        .newBuilder()
        .setSessionHandle(sessionHandle)
        .setTableName(tableName)
        .setColumnName(columnName)
        .setSchemaName(schemaName)
        .build()
    )
    verifySuccess(resp.getStatus)
    resp.getOperationHandle
  }

  /**
   * Used for JDBC to get Schema of the specified operation.
   */
  def getResultSetMetadata(operationHandle: BOperationHandle): BTableSchema = {
    val resp = this.rpcClient.invokeSync[BGetResultSetMetadata.BGetResultSetMetadataResp](
      BGetResultSetMetadata.BGetResultSetMetadataReq
        .newBuilder()
        .setOperationHandle(operationHandle)
        .build()
    )
    verifySuccess(resp.getStatus)
    resp.getSchema
  }

  /**
   * Used to verify whether the RPC result is correct.
   */
  private def verifySuccess(status: BStatus): Unit =
    if (status.getStatusCode != BStatusCode.B_STATUS_CODE_SUCCESS_STATUS) {
      throw BSQLException(
        status.getErrorMessage,
        status.getSqlState,
        status.getErrorCode
      )
    }
}

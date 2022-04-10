/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc.processor

import com.alipay.sofa.jraft.rpc.{ RpcContext, RpcRequestClosure, RpcRequestProcessor }
import org.bitlap.network.proto.driver.BCloseSession.{ BCloseSessionReq, BCloseSessionResp }
import org.bitlap.network.proto.driver.BExecuteStatement.{ BExecuteStatementReq, BExecuteStatementResp }
import org.bitlap.network.proto.driver.BFetchResults.{ BFetchResultsReq, BFetchResultsResp }
import org.bitlap.network.proto.driver.BGetColumns.{ BGetColumnsReq, BGetColumnsResp }
import org.bitlap.network.proto.driver.BGetResultSetMetadata.{ BGetResultSetMetadataReq, BGetResultSetMetadataResp }
import org.bitlap.network.proto.driver.BGetSchemas.{ BGetSchemasReq, BGetSchemasResp }
import org.bitlap.network.proto.driver.BGetTables.{ BGetTablesReq, BGetTablesResp }
import org.bitlap.network.proto.driver.BOpenSession.{ BOpenSessionReq, BOpenSessionResp }
import org.bitlap.network.types.{ handles, OperationType }
import org.bitlap.network.types.handles.OperationHandle
import org.bitlap.network.types.status.{ error, success }
import org.bitlap.tools.apply
import org.bitlap.tools.method.ProcessorCreator

import java.util.concurrent.Executor
import scala.jdk.CollectionConverters._

/**
 * @author 梦境迷离
 * @version 1.0,2021/12/11
 */
@apply
class JdbcProcessors {

  private implicit val s: JdbcProcessor = new JdbcProcessor()
  private implicit val e: Executor = null

  val openSession = ProcessorCreator[
    RpcRequestClosure,
    RpcRequestProcessor,
    RpcContext,
    BOpenSessionReq,
    BOpenSessionResp,
    JdbcProcessor,
    Executor
  ](
    BOpenSessionResp.getDefaultInstance,
    (service, _, req) => {
      val username = req.getUsername
      val password = req.getPassword
      val configurationMap = req.getConfigurationMap
      val ret =
        service.openSession(username, password, configurationMap.asScala.toMap)
      BOpenSessionResp
        .newBuilder()
        .setSessionHandle(ret.toBSessionHandle())
        .build()
    },
    (_, _, exception) => BOpenSessionResp.newBuilder().setStatus(error(exception)).build()
  )

  val closeSession = ProcessorCreator[
    RpcRequestClosure,
    RpcRequestProcessor,
    RpcContext,
    BCloseSessionReq,
    BCloseSessionResp,
    JdbcProcessor,
    Executor
  ](
    BCloseSessionResp.getDefaultInstance,
    (service, _, req) => {
      val sessionHandle = req.getSessionHandle
      service.closeSession(new handles.SessionHandle(sessionHandle))
      BCloseSessionResp.newBuilder().setStatus(success()).build()
    },
    (_, _, exception) => BCloseSessionResp.newBuilder().setStatus(error(exception)).build()
  )

  val fetchResults = ProcessorCreator[
    RpcRequestClosure,
    RpcRequestProcessor,
    RpcContext,
    BFetchResultsReq,
    BFetchResultsResp,
    JdbcProcessor,
    Executor
  ](
    BFetchResultsResp.getDefaultInstance,
    (service, _, req) => {
      val operationHandle = req.getOperationHandle
      val result =
        service.fetchResults(new handles.OperationHandle(operationHandle))
      BFetchResultsResp
        .newBuilder()
        .setHasMoreRows(false)
        .setStatus(success())
        .setResults(result.toBRowSet())
        .build()
    },
    (_, _, exception) => BFetchResultsResp.newBuilder().setStatus(error(exception)).build()
  )

  val getColumns = ProcessorCreator[
    RpcRequestClosure,
    RpcRequestProcessor,
    RpcContext,
    BGetColumnsReq,
    BGetColumnsResp,
    JdbcProcessor,
    Executor
  ](
    BGetColumnsResp.getDefaultInstance,
    (service, _, req) => {
      val result =
        service.getColumns(
          new handles.SessionHandle(req.getSessionHandle),
          req.getTableName,
          req.getSchemaName,
          req.getColumnName
        )
      BGetColumnsResp
        .newBuilder()
        .setStatus(success())
        .setOperationHandle(result.toBOperationHandle())
        .build()
    },
    (_, _, exception) => BGetColumnsResp.newBuilder().setStatus(error(exception)).build()
  )

  val getResultSet = ProcessorCreator[
    RpcRequestClosure,
    RpcRequestProcessor,
    RpcContext,
    BGetResultSetMetadataReq,
    BGetResultSetMetadataResp,
    JdbcProcessor,
    Executor
  ](
    BGetResultSetMetadataResp.getDefaultInstance,
    (service, _, req) => {
      val operationHandle = req.getOperationHandle
      val result = service.getResultSetMetadata(
        new handles.OperationHandle(operationHandle)
      )
      BGetResultSetMetadataResp
        .newBuilder()
        .setStatus(success())
        .setSchema(result.toBTableSchema())
        .build()
    },
    (_, _, exception) => BGetResultSetMetadataResp.newBuilder().setStatus(error(exception)).build()
  )

  val executeStatement = ProcessorCreator[
    RpcRequestClosure,
    RpcRequestProcessor,
    RpcContext,
    BExecuteStatementReq,
    BExecuteStatementResp,
    JdbcProcessor,
    Executor
  ](
    BExecuteStatementResp.getDefaultInstance,
    (service, _, req) => {
      val sessionHandle = req.getSessionHandle
      val statement = req.getStatement
      val confOverlayMap = req.getConfOverlayMap.asScala.toMap
      val operationHandle = service.executeStatement(
        sessionHandle = new handles.SessionHandle(sessionHandle),
        statement = statement,
        confOverlay = confOverlayMap
      )
      BExecuteStatementResp
        .newBuilder()
        .setOperationHandle(operationHandle.toBOperationHandle())
        .setStatus(success())
        .build()
    },
    (_, _, exception) => BExecuteStatementResp.newBuilder().setStatus(error(exception)).build()
  )

  val getSchemas = ProcessorCreator[
    RpcRequestClosure,
    RpcRequestProcessor,
    RpcContext,
    BGetSchemasReq,
    BGetSchemasResp,
    JdbcProcessor,
    Executor
  ](
    BGetSchemasResp.getDefaultInstance,
    (service, _, req) => {
      val session = new handles.SessionHandle(req.getSessionHandle)
      val result = service.getDatabases("")
      val handle = new OperationHandle(OperationType.GET_SCHEMAS)
      BGetSchemasResp
        .newBuilder()
        .setStatus(success())
        .setOperationHandle(handle.toBOperationHandle())
        .build()
    },
    (_, _, exception) => BGetSchemasResp.newBuilder().setStatus(error(exception)).build()
  )

  val getTables = ProcessorCreator[
    RpcRequestClosure,
    RpcRequestProcessor,
    RpcContext,
    BGetTablesReq,
    BGetTablesResp,
    JdbcProcessor,
    Executor
  ](
    BGetTablesResp.getDefaultInstance,
    (service, _, req) => {
      val result = {
        val session = new handles.SessionHandle(req.getSessionHandle)
        service.getTables(req.getSchemaName, req.getTableName)
      }
      val handle = new OperationHandle(OperationType.GET_TABLES)
      BGetTablesResp
        .newBuilder()
        .setStatus(success())
        .setOperationHandle(handle.toBOperationHandle())
        .build()
    },
    (_, _, exception) => BGetTablesResp.newBuilder().setStatus(error(exception)).build()
  )
}

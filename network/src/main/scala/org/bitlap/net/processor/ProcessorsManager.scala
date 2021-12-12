package org.bitlap.net.processor

import com.alipay.sofa.jraft.rpc.{ RpcContext, RpcRequestClosure, RpcRequestProcessor }
import org.bitlap.net.NetworkServiceImpl
import org.bitlap.network.proto.driver.BOpenSession.{ BOpenSessionReq, BOpenSessionResp }
import org.bitlap.tools.{ apply, ProcessorCreator }
import org.bitlap.net.handles

import java.util.concurrent.Executor
import scala.jdk.CollectionConverters.MapHasAsScala
import org.bitlap.network.proto.driver.BCloseSession.{ BCloseSessionReq, BCloseSessionResp }
import org.bitlap.network.proto.driver.BExecuteStatement.{ BExecuteStatementReq, BExecuteStatementResp }
import org.bitlap.network.proto.driver.BFetchResults.{ BFetchResultsReq, BFetchResultsResp }
import org.bitlap.network.proto.driver.BGetColumns.{ BGetColumnsReq, BGetColumnsResp }
import org.bitlap.network.proto.driver.BGetResultSetMetadata.{ BGetResultSetMetadataReq, BGetResultSetMetadataResp }
import org.bitlap.network.proto.driver.BGetSchemas.{ BGetSchemasReq, BGetSchemasResp }
import org.bitlap.network.proto.driver.BGetTables.{ BGetTablesReq, BGetTablesResp }

/**
 *
 * @author 梦境迷离
 * @version 1.0,2021/12/11
 */
@apply
class ProcessorsManager(private val service: NetworkServiceImpl, val executor: Executor) {

  private implicit val s: NetworkServiceImpl = service
  private implicit val e: Executor = executor

  val openSession = ProcessorCreator[RpcRequestClosure, RpcRequestProcessor,
    RpcContext, BOpenSessionReq,
    BOpenSessionResp, NetworkServiceImpl, Executor](
    BOpenSessionResp.getDefaultInstance,
    (service, _, req) => {
      val username = req.getUsername
      val password = req.getPassword
      val configurationMap = req.getConfigurationMap
      val ret = service.openSession(username, password, configurationMap.asScala.toMap)
      BOpenSessionResp.newBuilder().setSessionHandle(ret.toBSessionHandle()).build()
    },
    (_, _, exception) => {
      BOpenSessionResp.newBuilder().setStatus(error(exception)).build()
    }
  )

  val closeSession = ProcessorCreator[RpcRequestClosure, RpcRequestProcessor,
    RpcContext, BCloseSessionReq,
    BCloseSessionResp, NetworkServiceImpl, Executor](
    BCloseSessionResp.getDefaultInstance,
    (service, _, req) => {
      val sessionHandle = req.getSessionHandle
      service.closeSession(new handles.SessionHandle(sessionHandle))
      BCloseSessionResp.newBuilder().setStatus(success()).build()
    },
    (_, _, exception) => {
      BCloseSessionResp.newBuilder().setStatus(error(exception)).build()
    }
  )

  val fetchResults = ProcessorCreator[RpcRequestClosure, RpcRequestProcessor,
    RpcContext, BFetchResultsReq,
    BFetchResultsResp, NetworkServiceImpl, Executor](
    BFetchResultsResp.getDefaultInstance,
    (service, _, req) => {
      val operationHandle = req.getOperationHandle
      val result = service.fetchResults(new handles.OperationHandle(operationHandle))
      BFetchResultsResp.newBuilder()
        .setHasMoreRows(false)
        .setStatus(success()).setResults(result.toBRowSet()).build()
    },
    (_, _, exception) => {
      BFetchResultsResp.newBuilder().setStatus(error(exception)).build()
    }
  )

  val getColumns = ProcessorCreator[RpcRequestClosure, RpcRequestProcessor,
    RpcContext, BGetColumnsReq,
    BGetColumnsResp, NetworkServiceImpl, Executor](
    BGetColumnsResp.getDefaultInstance,
    (service, _, req) => {
      val result =
        service.getColumns(
          new handles.SessionHandle(req.getSessionHandle),
          req.getTableName,
          req.getSchemaName,
          req.getColumnName
        )
      BGetColumnsResp.newBuilder()
        .setStatus(success()).setOperationHandle(result.toBOperationHandle()).build()
    },
    (_, _, exception) => {
      BGetColumnsResp.newBuilder().setStatus(error(exception)).build()
    }
  )

  val getResultSet = ProcessorCreator[RpcRequestClosure, RpcRequestProcessor,
    RpcContext, BGetResultSetMetadataReq,
    BGetResultSetMetadataResp, NetworkServiceImpl, Executor](
    BGetResultSetMetadataResp.getDefaultInstance,
    (service, _, req) => {
      val operationHandle = req.getOperationHandle
      val result = service.getResultSetMetadata(new handles.OperationHandle(operationHandle))
      BGetResultSetMetadataResp.newBuilder().setStatus(success()).setSchema(result.toBTableSchema()).build()
    },
    (_, _, exception) => {
      BGetResultSetMetadataResp.newBuilder().setStatus(error(exception)).build()
    }
  )

  val executeStatement = ProcessorCreator[RpcRequestClosure, RpcRequestProcessor,
    RpcContext, BExecuteStatementReq,
    BExecuteStatementResp, NetworkServiceImpl, Executor](
    BExecuteStatementResp.getDefaultInstance,
    (service, _, req) => {
      val sessionHandle = req.getSessionHandle
      val statement = req.getStatement
      val confOverlayMap = req.getConfOverlayMap.asScala.toMap
      val operationHandle = service.executeStatement(
        sessionHandle = new handles.SessionHandle(sessionHandle),
        statement = statement, confOverlay = confOverlayMap
      )
      BExecuteStatementResp.newBuilder()
        .setOperationHandle(operationHandle.toBOperationHandle())
        .setStatus(success()).build()
    },
    (_, _, exception) => {
      BExecuteStatementResp.newBuilder().setStatus(error(exception)).build()
    }
  )

  val getSchemas = ProcessorCreator[RpcRequestClosure, RpcRequestProcessor,
    RpcContext, BGetSchemasReq,
    BGetSchemasResp, NetworkServiceImpl, Executor](
    BGetSchemasResp.getDefaultInstance,
    (service, _, req) => {
      val sessionHandle = req.getSessionHandle
      val result = service.getSchemas(new handles.SessionHandle(sessionHandle))
      BGetSchemasResp.newBuilder()
        .setStatus(success()).setOperationHandle(result.toBOperationHandle()).build()
    },
    (_, _, exception) => {
      BGetSchemasResp.newBuilder().setStatus(error(exception)).build()
    }
  )

  val getTables = ProcessorCreator[RpcRequestClosure, RpcRequestProcessor,
    RpcContext, BGetTablesReq,
    BGetTablesResp, NetworkServiceImpl, Executor](
    BGetTablesResp.getDefaultInstance,
    (service, _, req) => {
      val result =
        service.getTables(new handles.SessionHandle((req.getSessionHandle))
          , req.getTableName, req.getSchemaName)
      BGetTablesResp.newBuilder()
        .setStatus(success()).setOperationHandle(result.toBOperationHandle()).build()
    },
    (_, _, exception) => {
      BGetTablesResp.newBuilder().setStatus(error(exception)).build()
    }
  )
}

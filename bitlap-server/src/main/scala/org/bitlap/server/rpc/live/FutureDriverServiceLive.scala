/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc.live

import io.grpc.Status
import org.bitlap.network.driver.proto.BCloseSession.{ BCloseSessionReq, BCloseSessionResp }
import org.bitlap.network.driver.proto.BExecuteStatement.{ BExecuteStatementReq, BExecuteStatementResp }
import org.bitlap.network.driver.proto.BFetchResults.{ BFetchResultsReq, BFetchResultsResp }
import org.bitlap.network.driver.proto.BGetColumns.{ BGetColumnsReq, BGetColumnsResp }
import org.bitlap.network.driver.proto.BGetResultSetMetadata.{ BGetResultSetMetadataReq, BGetResultSetMetadataResp }
import org.bitlap.network.driver.proto.BGetSchemas.{ BGetSchemasReq, BGetSchemasResp }
import org.bitlap.network.driver.proto.BGetTables.{ BGetTablesReq, BGetTablesResp }
import org.bitlap.network.driver.proto.BOpenSession.{ BOpenSessionReq, BOpenSessionResp }
import org.bitlap.network.driver.service.ZioService.ZDriverService
import org.bitlap.network.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.{ RpcFuture, RpcStatusBuilder }
import zio.ZIO

/** A zio-grpc server implement by future backend.
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
case class FutureDriverServiceLive(private val futureRpcBackend: RpcFuture)
    extends ZDriverService[Any, Any]
    with RpcStatusBuilder {

  def openSession(request: BOpenSessionReq): ZIO[Any, Status, BOpenSessionResp] =
    futureRpcBackend.transform {
      _.openSession(request.username, request.password, request.configuration)
    } { hd =>
      BOpenSessionResp(
        successOpt(),
        configuration = request.configuration,
        sessionHandle = Some(hd.toBSessionHandle())
      )
    }

  override def closeSession(request: BCloseSessionReq): ZIO[Any, Status, BCloseSessionResp] =
    futureRpcBackend.transform {
      _.closeSession(new SessionHandle(request.getSessionHandle))
    }(_ => BCloseSessionResp(successOpt()))

  override def executeStatement(request: BExecuteStatementReq): ZIO[Any, Status, BExecuteStatementResp] =
    futureRpcBackend.transform {
      _.executeStatement(
        new SessionHandle(request.getSessionHandle),
        request.statement,
        request.queryTimeout,
        request.confOverlay
      )
    } { hd =>
      BExecuteStatementResp(successOpt(), Some(hd.toBOperationHandle()))
    }

  override def fetchResults(request: BFetchResultsReq): ZIO[Any, Status, BFetchResultsResp] =
    futureRpcBackend.transform(_.fetchResults(new OperationHandle(request.getOperationHandle))) { hd =>
      BFetchResultsResp(hd.status.map(_.toBStatus), hd.hasMoreRows, Some(hd.results.toBRowSet))
    }

  override def getSchemas(request: BGetSchemasReq): ZIO[Any, Status, BGetSchemasResp] =
    futureRpcBackend.transform {
      _.getSchemas(new SessionHandle(request.getSessionHandle), request.catalogName, request.schemaName)
    } { hd =>
      BGetSchemasResp(
        successOpt(),
        Some(hd.toBOperationHandle())
      )
    }

  override def getTables(request: BGetTablesReq): ZIO[Any, Status, BGetTablesResp] =
    // FIXME 参数名不同
    // TODO 返回了 operationHandle，不是具体数据，是不是还要请求一次？改成直接返回数据？
    futureRpcBackend.transform(_.getTables(request.schemaName, request.tableName)) { hd =>
      BGetTablesResp(
        successOpt(),
        Some(hd.toBOperationHandle())
      )
    }

  override def getColumns(request: BGetColumnsReq): ZIO[Any, Status, BGetColumnsResp] =
    futureRpcBackend.transform {
      _.getColumns(
        new SessionHandle(request.getSessionHandle),
        request.schemaName,
        request.tableName,
        request.columnName
      )
    } { hd =>
      BGetColumnsResp(
        successOpt(),
        Some(hd.toBOperationHandle())
      )
    }

  override def getResultSetMetadata(request: BGetResultSetMetadataReq): ZIO[Any, Status, BGetResultSetMetadataResp] =
    futureRpcBackend.transform {
      _.getResultSetMetadata(new OperationHandle(request.getOperationHandle))
    } { hd =>
      BGetResultSetMetadataResp(
        successOpt(),
        Some(hd.toBTableSchema)
      )
    }
}

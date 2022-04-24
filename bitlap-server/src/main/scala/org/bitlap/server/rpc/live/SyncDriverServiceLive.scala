/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc.live

import io.grpc.Status
import org.bitlap.network.RpcStatus
import org.bitlap.network.driver.proto.BCloseSession.{ BCloseSessionReq, BCloseSessionResp }
import org.bitlap.network.driver.proto.BExecuteStatement.{ BExecuteStatementReq, BExecuteStatementResp }
import org.bitlap.network.driver.proto.BFetchResults.{ BFetchResultsReq, BFetchResultsResp }
import org.bitlap.network.driver.proto.BGetColumns.{ BGetColumnsReq, BGetColumnsResp }
import org.bitlap.network.driver.proto.BGetResultSetMetadata.{ BGetResultSetMetadataReq, BGetResultSetMetadataResp }
import org.bitlap.network.driver.proto.BGetSchemas.{ BGetSchemasReq, BGetSchemasResp }
import org.bitlap.network.driver.proto.BGetTables.{ BGetTablesReq, BGetTablesResp }
import org.bitlap.network.driver.proto.BOpenSession.{ BOpenSessionReq, BOpenSessionResp }
import org.bitlap.network.driver.service.ZioService.ZDriverService
import org.bitlap.network.dsl.zioFrom
import org.bitlap.network.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.rpc.{ Identity, RpcF }
import org.bitlap.server.rpc.backend.SyncRpcBackend
import zio.{ IO, ZIO }

/**
 * A zio-grpc server implement by sync backend.
 *
 * @author 梦境迷离
 * @version 1.0,2022/4/21
 */
case class SyncDriverServiceLive() extends ZDriverService[Any, Any] with RpcStatus {

  private lazy val syncRpcBackend: RpcF[Identity] = new SyncRpcBackend

  def openSession(request: BOpenSessionReq): IO[Status, BOpenSessionResp] = zioFrom {
    val handle = syncRpcBackend.openSession(request.username, request.password, request.configuration)
    BOpenSessionResp(
      successOpt(),
      configuration = request.configuration,
      sessionHandle = Some(handle.toBSessionHandle())
    )
  }
  override def closeSession(request: BCloseSessionReq): ZIO[Any, Status, BCloseSessionResp] = zioFrom {
    syncRpcBackend.closeSession(new SessionHandle(request.getSessionHandle))
    BCloseSessionResp(successOpt())

  }

  override def executeStatement(request: BExecuteStatementReq): ZIO[Any, Status, BExecuteStatementResp] = zioFrom {
    val handle = syncRpcBackend.executeStatement(
      new SessionHandle(request.getSessionHandle),
      request.statement,
      request.queryTimeout,
      request.confOverlay
    )
    BExecuteStatementResp(successOpt(), Some(handle.toBOperationHandle()))
  }

  override def fetchResults(
    request: BFetchResultsReq
  ): ZIO[Any, Status, BFetchResultsResp] =
    zioFrom {
      syncRpcBackend.fetchResults(new OperationHandle(request.getOperationHandle)).toBFetchResults
    }

  override def getSchemas(request: BGetSchemasReq): ZIO[Any, Status, BGetSchemasResp] = ???

  override def getTables(request: BGetTablesReq): ZIO[Any, Status, BGetTablesResp] = ???

  override def getColumns(request: BGetColumnsReq): ZIO[Any, Status, BGetColumnsResp] = ???

  override def getResultSetMetadata(
    request: BGetResultSetMetadataReq
  ): ZIO[Any, Status, BGetResultSetMetadataResp] = zioFrom {
    val handle = syncRpcBackend.getResultSetMetadata(new OperationHandle(request.getOperationHandle))
    BGetResultSetMetadataResp(successOpt(), Some(handle.toBTableSchema))

  }
}

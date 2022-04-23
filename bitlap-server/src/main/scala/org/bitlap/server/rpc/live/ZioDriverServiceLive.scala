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
import org.bitlap.network.function.errorApplyFunc
import org.bitlap.network.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.rpc.RpcN
import org.bitlap.server.rpc.backend.ZioRpcBackend
import zio.ZIO

/**
 * A zio-grpc server implement by zio backend.
 *
 * @author 梦境迷离
 * @version 1.0,2022/4/21
 */
case class ZioDriverServiceLive() extends ZDriverService[Any, Any] with RpcStatus {

  private lazy val zioRpcBackend: RpcN[ZIO] = new ZioRpcBackend()

  def openSession(request: BOpenSessionReq): ZIO[Any, Status, BOpenSessionResp] =
    zioRpcBackend
      .openSession(request.username, request.password, request.configuration)
      .map(shd =>
        BOpenSessionResp(
          successOpt(),
          configuration = request.configuration,
          sessionHandle = Some(shd.toBSessionHandle())
        )
      )
      .mapError(errorApplyFunc)

  override def closeSession(request: BCloseSessionReq): ZIO[Any, Status, BCloseSessionResp] =
    zioRpcBackend
      .closeSession(new SessionHandle(request.getSessionHandle))
      .map(_ => BCloseSessionResp(successOpt()))
      .mapError(errorApplyFunc)

  override def executeStatement(request: BExecuteStatementReq): ZIO[Any, Status, BExecuteStatementResp] =
    zioRpcBackend
      .executeStatement(
        new SessionHandle(request.getSessionHandle),
        request.statement,
        request.queryTimeout,
        request.confOverlay
      )
      .map(hd => BExecuteStatementResp(successOpt(), Some(hd.toBOperationHandle())))
      .mapError(errorApplyFunc)

  override def fetchResults(request: BFetchResultsReq): ZIO[Any, Status, BFetchResultsResp] =
    zioRpcBackend
      .fetchResults(new OperationHandle(request.getOperationHandle))
      .map(_.toBFetchResults)
      .mapError(errorApplyFunc)

  override def getSchemas(request: BGetSchemasReq): ZIO[Any, Status, BGetSchemasResp] = ???

  override def getTables(request: BGetTablesReq): ZIO[Any, Status, BGetTablesResp] = ???

  override def getColumns(request: BGetColumnsReq): ZIO[Any, Status, BGetColumnsResp] = ???

  override def getResultSetMetadata(request: BGetResultSetMetadataReq): ZIO[Any, Status, BGetResultSetMetadataResp] =
    zioRpcBackend
      .getResultSetMetadata(new OperationHandle(request.getOperationHandle))
      .map(t => BGetResultSetMetadataResp(successOpt(), Some(t.toBTableSchema)))
      .mapError(errorApplyFunc)
}

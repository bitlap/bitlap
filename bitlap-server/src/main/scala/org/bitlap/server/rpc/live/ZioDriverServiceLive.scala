/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc.live

import io.grpc.Status
import org.bitlap.network.{ Monad, RpcStatus }
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

  private lazy val zioRpcBackend: RpcN[ZIO] = ZioRpcBackend()

  def openSession(request: BOpenSessionReq): ZIO[Any, Status, BOpenSessionResp] =
    Monad.mapBoth(zioRpcBackend) {
      _.openSession(request.username, request.password, request.configuration)
    } { shd =>
      BOpenSessionResp(
        successOpt(),
        configuration = request.configuration,
        sessionHandle = Some(shd.toBSessionHandle())
      )
    }

  override def closeSession(request: BCloseSessionReq): ZIO[Any, Status, BCloseSessionResp] =
    Monad.mapBoth(zioRpcBackend) {
      _.closeSession(new SessionHandle(request.getSessionHandle))
    }(_ => BCloseSessionResp(successOpt()))

  override def executeStatement(request: BExecuteStatementReq): ZIO[Any, Status, BExecuteStatementResp] =
    Monad.mapBoth(zioRpcBackend) {
      _.executeStatement(
        new SessionHandle(request.getSessionHandle),
        request.statement,
        request.queryTimeout,
        request.confOverlay
      )
    }(hd => BExecuteStatementResp(successOpt(), Some(hd.toBOperationHandle())))

  override def fetchResults(request: BFetchResultsReq): ZIO[Any, Status, BFetchResultsResp] =
    Monad.mapBoth(zioRpcBackend) {
      _.fetchResults(new OperationHandle(request.getOperationHandle))
    }(_.toBFetchResults)

  override def getSchemas(request: BGetSchemasReq): ZIO[Any, Status, BGetSchemasResp] = ???

  override def getTables(request: BGetTablesReq): ZIO[Any, Status, BGetTablesResp] = ???

  override def getColumns(request: BGetColumnsReq): ZIO[Any, Status, BGetColumnsResp] = ???

  override def getResultSetMetadata(request: BGetResultSetMetadataReq): ZIO[Any, Status, BGetResultSetMetadataResp] =
    Monad.mapBoth((zioRpcBackend)) {
      _.getResultSetMetadata(new OperationHandle(request.getOperationHandle))
    }(t => BGetResultSetMetadataResp(successOpt(), Some(t.toBTableSchema)))
}

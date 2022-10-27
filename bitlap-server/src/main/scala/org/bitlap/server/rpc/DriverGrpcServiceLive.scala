/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc

import io.grpc._
import org.bitlap.network.driver.proto.BCloseSession.{ BCloseSessionReq, BCloseSessionResp }
import org.bitlap.network.driver.proto.BExecuteStatement.{ BExecuteStatementReq, BExecuteStatementResp }
import org.bitlap.network.driver.proto.BFetchResults.{ BFetchResultsReq, BFetchResultsResp }
import org.bitlap.network.driver.proto.BGetColumns.{ BGetColumnsReq, BGetColumnsResp }
import org.bitlap.network.driver.proto.BGetResultSetMetadata.{ BGetResultSetMetadataReq, BGetResultSetMetadataResp }
import org.bitlap.network.driver.proto.BGetSchemas.{ BGetSchemasReq, BGetSchemasResp }
import org.bitlap.network.driver.proto.BGetTables.{ BGetTablesReq, BGetTablesResp }
import org.bitlap.network.driver.proto.BOpenSession.{ BOpenSessionReq, BOpenSessionResp }
import org.bitlap.network.driver.service.ZioService._
import org.bitlap.network.handles._
import org.bitlap.network.{ errorApplyFunc, AsyncRpc, RpcStatus }
import org.bitlap.tools._
import zio._

/** A zio-grpc server implement by zio backend.
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
@apply
final class DriverGrpcServiceLive(private val zioRpcBackend: AsyncRpc) extends ZDriverService[Any, Any] with RpcStatus {

  def openSession(request: BOpenSessionReq): ZIO[Any, Status, BOpenSessionResp] =
    zioRpcBackend
      .map(_.openSession(request.username, request.password, request.configuration)) { shd =>
        BOpenSessionResp(
          successOpt(),
          configuration = request.configuration,
          sessionHandle = Some(shd.toBSessionHandle())
        )
      }
      .mapError(errorApplyFunc)

  override def closeSession(request: BCloseSessionReq): ZIO[Any, Status, BCloseSessionResp] =
    zioRpcBackend
      .map(_.closeSession(new SessionHandle(request.getSessionHandle))) { _ =>
        BCloseSessionResp(successOpt())
      }
      .mapError(errorApplyFunc)

  override def executeStatement(request: BExecuteStatementReq): ZIO[Any, Status, BExecuteStatementResp] =
    zioRpcBackend.map {
      _.executeStatement(
        new SessionHandle(request.getSessionHandle),
        request.statement,
        request.queryTimeout,
        request.confOverlay
      )
    }(hd => BExecuteStatementResp(successOpt(), Some(hd.toBOperationHandle())))
      .mapError(errorApplyFunc)

  override def fetchResults(request: BFetchResultsReq): ZIO[Any, Status, BFetchResultsResp] =
    zioRpcBackend.map {
      _.fetchResults(
        new OperationHandle(request.getOperationHandle),
        request.maxRows.toInt,
        request.fetchType
      )
    }(_.toBFetchResults)
      .mapError(errorApplyFunc)

  override def getSchemas(request: BGetSchemasReq): ZIO[Any, Status, BGetSchemasResp] = ???

  override def getTables(request: BGetTablesReq): ZIO[Any, Status, BGetTablesResp] = ???

  override def getColumns(request: BGetColumnsReq): ZIO[Any, Status, BGetColumnsResp] = ???

  override def getResultSetMetadata(request: BGetResultSetMetadataReq): ZIO[Any, Status, BGetResultSetMetadataResp] =
    zioRpcBackend.map {
      _.getResultSetMetadata(new OperationHandle(request.getOperationHandle))
    }(t => BGetResultSetMetadataResp(successOpt(), Some(t.toBTableSchema)))
      .mapError(errorApplyFunc)

}

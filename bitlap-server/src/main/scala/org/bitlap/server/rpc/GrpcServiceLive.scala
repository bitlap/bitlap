/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc

import io.grpc._
import org.bitlap.network.NetworkException._
import org.bitlap.network._
import org.bitlap.network.driver.proto.BCloseSession.{ BCloseSessionReq, BCloseSessionResp }
import org.bitlap.network.driver.proto.BExecuteStatement.{ BExecuteStatementReq, BExecuteStatementResp }
import org.bitlap.network.driver.proto.BFetchResults.{ BFetchResultsReq, BFetchResultsResp }
import org.bitlap.network.driver.proto.BGetDatabases.BGetDatabasesResp
import org.bitlap.network.driver.proto.BGetResultSetMetadata.{ BGetResultSetMetadataReq, BGetResultSetMetadataResp }
import org.bitlap.network.driver.proto.BGetTables.{ BGetTablesReq, BGetTablesResp }
import org.bitlap.network.driver.proto.BOpenSession.{ BOpenSessionReq, BOpenSessionResp }
import org.bitlap.network.driver.proto._
import org.bitlap.network.driver.service.ZioService._
import org.bitlap.network.handles._
import org.bitlap.server._
import org.bitlap.tools._
import zio._

/** RPC的服务端API实现，基于 zio-grpc,zio 1.0
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
@apply
final class GrpcServiceLive(private val asyncRpcBackend: AsyncRpc) extends ZDriverService[Any, Any] with RpcStatus {

  def openSession(request: BOpenSessionReq): ZIO[Any, Status, BOpenSessionResp] =
    asyncRpcBackend
      .map(_.openSession(request.username, request.password, request.configuration)) { shd =>
        BOpenSessionResp(
          successOpt(),
          configuration = request.configuration,
          sessionHandle = Some(shd.toBSessionHandle())
        )
      }
      .mapError(errorApplyFunc)

  override def closeSession(request: BCloseSessionReq): ZIO[Any, Status, BCloseSessionResp] =
    asyncRpcBackend
      .map(_.closeSession(new SessionHandle(request.getSessionHandle))) { _ =>
        BCloseSessionResp(successOpt())
      }
      .mapError(errorApplyFunc)

  override def executeStatement(request: BExecuteStatementReq): ZIO[Any, Status, BExecuteStatementResp] =
    asyncRpcBackend.map {
      _.executeStatement(
        new SessionHandle(request.getSessionHandle),
        request.statement,
        request.queryTimeout,
        request.confOverlay
      )
    }(hd => BExecuteStatementResp(successOpt(), Some(hd.toBOperationHandle())))
      .mapError(errorApplyFunc)

  override def fetchResults(request: BFetchResultsReq): ZIO[Any, Status, BFetchResultsResp] =
    asyncRpcBackend.map {
      _.fetchResults(
        new OperationHandle(request.getOperationHandle),
        request.maxRows.toInt,
        request.fetchType
      )
    }(_.toBFetchResults)
      .mapError(errorApplyFunc)

  override def getResultSetMetadata(request: BGetResultSetMetadataReq): ZIO[Any, Status, BGetResultSetMetadataResp] =
    asyncRpcBackend.map {
      _.getResultSetMetadata(new OperationHandle(request.getOperationHandle))
    }(t => BGetResultSetMetadataResp(successOpt(), Some(t.toBTableSchema)))
      .mapError(errorApplyFunc)

  override def getDatabases(
    request: BGetDatabases.BGetDatabasesReq
  ): ZIO[Any, Status, BGetDatabases.BGetDatabasesResp] =
    asyncRpcBackend.map {
      _.getDatabases(new SessionHandle(request.getSessionHandle), request.pattern)
    }(t => BGetDatabasesResp(successOpt(), Option(t.toBOperationHandle())))
      .mapError(errorApplyFunc)

  override def getTables(request: BGetTablesReq): ZIO[Any, Status, BGetTablesResp] =
    asyncRpcBackend.map {
      _.getTables(new SessionHandle(request.getSessionHandle), request.database, request.pattern)
    }(t => BGetTablesResp(successOpt(), Option(t.toBOperationHandle())))
      .mapError(errorApplyFunc)

  override def getLeader(request: BGetRaftMetadata.BGetLeaderReq): ZIO[Any, Status, BGetRaftMetadata.BGetLeaderResp] = {
    val leaderAddress = BitlapServerContext.getLeaderAddress()
    leaderAddress.flatMap { ld =>
      if (ld == null || ld.port <= 0 || ld.ip == null || ld.ip.isEmpty) {
        Task.fail(LeaderServerNotFoundException(s"requestId: ${request.requestId}"))
      } else {
        Task.succeed(ld)
      }
    }.mapBoth(
      errorApplyFunc,
      t => BGetRaftMetadata.BGetLeaderResp(Option(t.ip), t.port)
    )
  }

}

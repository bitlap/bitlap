/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.rpc

import org.bitlap.network.*
import org.bitlap.network.Driver.*
import org.bitlap.network.Driver.ZioDriver.ZDriverService
import org.bitlap.network.NetworkException.*
import org.bitlap.network.enumeration.GetInfoType
import org.bitlap.network.handles.*
import org.bitlap.server.*

import io.grpc.*
import scalapb.zio_grpc.RequestContext
import zio.*

/** RPC的服务端API实现，基于 zio-grpc,zio 2.0
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/21
 */
object GrpcServiceLive:

  lazy val live: ZLayer[DriverIO, Nothing, GrpcServiceLive] =
    ZLayer.fromFunction((rpc: DriverIO) => new GrpcServiceLive(rpc))
end GrpcServiceLive

final class GrpcServiceLive(private val rpc: DriverIO) extends ZDriverService[RequestContext]:

  // 直接使用zio-grpc的Status表示错误 避免处理多重错误
  override def openSession(request: BOpenSessionReq, context: RequestContext): IO[StatusException, BOpenSessionResp] =
    rpc
      .when(
        BitlapContext.isLeader,
        OperationMustOnLeaderException(),
        _.openSession(request.username, request.password, request.configuration)
      )
      .mapBoth(
        errorApplyFunc,
        shd =>
          BOpenSessionResp(
            configuration = request.configuration,
            sessionHandle = Some(shd.toBSessionHandle())
          )
      )

  override def closeSession(request: BCloseSessionReq, context: RequestContext)
    : IO[StatusException, BCloseSessionResp] =
    rpc
      .when(
        BitlapContext.isLeader,
        OperationMustOnLeaderException(),
        _.closeSession(new SessionHandle(request.getSessionHandle))
      )
      .mapBoth(errorApplyFunc, _ => BCloseSessionResp())

  override def executeStatement(request: BExecuteStatementReq, context: RequestContext)
    : IO[StatusException, BExecuteStatementResp] =
    rpc
      .when(
        BitlapContext.isLeader,
        OperationMustOnLeaderException(),
        _.executeStatement(
          new SessionHandle(request.getSessionHandle),
          request.statement,
          request.queryTimeout,
          request.confOverlay
        )
      )
      .mapBoth(errorApplyFunc, hd => BExecuteStatementResp(Some(hd.toBOperationHandle())))

  override def fetchResults(request: BFetchResultsReq, context: RequestContext)
    : IO[StatusException, BFetchResultsResp] =
    rpc
      .when(
        BitlapContext.isLeader,
        OperationMustOnLeaderException(),
        _.fetchResults(new OperationHandle(request.getOperationHandle), request.maxRows.toInt, request.fetchType)
      )
      .mapBoth(errorApplyFunc, _.toBFetchResultsResp)

  override def getResultSetMetadata(request: BGetResultSetMetadataReq, context: RequestContext)
    : IO[StatusException, BGetResultSetMetadataResp] =
    rpc
      .when(
        BitlapContext.isLeader,
        OperationMustOnLeaderException(),
        _.getResultSetMetadata(new OperationHandle(request.getOperationHandle))
      )
      .mapBoth(errorApplyFunc, _.toBGetResultSetMetadataResp)

  override def getDatabases(
    request: BGetDatabasesReq,
    context: RequestContext
  ): IO[StatusException, BGetDatabasesResp] =
    rpc
      .when(
        BitlapContext.isLeader,
        OperationMustOnLeaderException(),
        _.getDatabases(new SessionHandle(request.getSessionHandle), request.pattern)
      )
      .mapBoth(errorApplyFunc, t => BGetDatabasesResp(Option(t.toBOperationHandle())))

  override def getTables(request: BGetTablesReq, context: RequestContext): IO[StatusException, BGetTablesResp] =
    rpc
      .when(
        BitlapContext.isLeader,
        OperationMustOnLeaderException(),
        _.getTables(new SessionHandle(request.getSessionHandle), request.database, request.pattern)
      )
      .mapBoth(errorApplyFunc, t => BGetTablesResp(Option(t.toBOperationHandle())))

  override def getLeader(request: BGetLeaderReq, context: RequestContext): IO[StatusException, BGetLeaderResp] = {
    val leaderAddress = BitlapContext.getLeaderAddress()
    leaderAddress.flatMap { ld =>
      if ld == null || ld.port <= 0 || ld.ip == null || ld.ip.isEmpty then {
        ZIO.fail(LeaderNotFoundException(s"requestId: ${request.requestId}"))
      } else {
        ZIO.succeed(ld)
      }
    }
      .mapBoth(
        errorApplyFunc,
        t => BGetLeaderResp(Option(t.ip), t.port)
      )
  }

  override def cancelOperation(
    request: BCancelOperationReq,
    context: RequestContext
  ): IO[StatusException, BCancelOperationResp] =
    rpc
      .when(
        BitlapContext.isLeader,
        OperationMustOnLeaderException(),
        _.cancelOperation(new OperationHandle(request.getOperationHandle))
      )
      .mapBoth(errorApplyFunc, _ => BCancelOperationResp())

  override def getOperationStatus(request: BGetOperationStatusReq, context: RequestContext)
    : IO[StatusException, BGetOperationStatusResp] =
    rpc
      .when(
        BitlapContext.isLeader,
        OperationMustOnLeaderException(),
        _.getOperationStatus(new OperationHandle(request.getOperationHandle))
      )
      .mapBoth(errorApplyFunc, _.toBGetOperationStatusResp)

  override def closeOperation(request: BCloseOperationReq, context: RequestContext)
    : IO[StatusException, BCloseOperationResp] =
    rpc
      .when(
        BitlapContext.isLeader,
        OperationMustOnLeaderException(),
        _.closeOperation(new OperationHandle(request.getOperationHandle))
      )
      .mapBoth(errorApplyFunc, _ => BCloseOperationResp())

  override def getInfo(request: BGetInfoReq, context: RequestContext): IO[StatusException, BGetInfoResp] =
    rpc
      .when(
        BitlapContext.isLeader,
        OperationMustOnLeaderException(),
        _.getInfo(new SessionHandle(request.getSessionHandle), GetInfoType.toGetInfoType(request.infoType))
      )
      .mapBoth(errorApplyFunc, _.toBGetInfoResp)

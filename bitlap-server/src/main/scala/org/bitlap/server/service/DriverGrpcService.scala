/* Copyright (c) 2023 bitlap.org */
package org.bitlap.server.service

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
object DriverGrpcService:

  lazy val live: ZLayer[DriverIO, Nothing, DriverGrpcService] =
    ZLayer.fromFunction((driverIO: DriverIO) => new DriverGrpcService(driverIO))
end DriverGrpcService

final class DriverGrpcService(private val driverIO: DriverIO) extends ZDriverService[RequestContext]:

  // 直接使用zio-grpc的Status表示错误 避免处理多重错误
  override def openSession(request: BOpenSessionReq, context: RequestContext): IO[StatusException, BOpenSessionResp] =
    driverIO
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
    driverIO
      .when(
        BitlapContext.isLeader,
        OperationMustOnLeaderException(),
        _.closeSession(new SessionHandle(request.getSessionHandle))
      )
      .mapBoth(errorApplyFunc, _ => BCloseSessionResp())

  override def executeStatement(request: BExecuteStatementReq, context: RequestContext)
    : IO[StatusException, BExecuteStatementResp] =
    driverIO
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
    driverIO
      .when(
        BitlapContext.isLeader,
        OperationMustOnLeaderException(),
        _.fetchResults(new OperationHandle(request.getOperationHandle), request.maxRows.toInt, request.fetchType)
      )
      .mapBoth(errorApplyFunc, _.toBFetchResultsResp)

  override def getResultSetMetadata(request: BGetResultSetMetadataReq, context: RequestContext)
    : IO[StatusException, BGetResultSetMetadataResp] =
    driverIO
      .when(
        BitlapContext.isLeader,
        OperationMustOnLeaderException(),
        _.getResultSetMetadata(new OperationHandle(request.getOperationHandle))
      )
      .mapBoth(errorApplyFunc, _.toBGetResultSetMetadataResp)

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
    driverIO
      .when(
        BitlapContext.isLeader,
        OperationMustOnLeaderException(),
        _.cancelOperation(new OperationHandle(request.getOperationHandle))
      )
      .mapBoth(errorApplyFunc, _ => BCancelOperationResp())

  override def getOperationStatus(request: BGetOperationStatusReq, context: RequestContext)
    : IO[StatusException, BGetOperationStatusResp] =
    driverIO
      .when(
        BitlapContext.isLeader,
        OperationMustOnLeaderException(),
        _.getOperationStatus(new OperationHandle(request.getOperationHandle))
      )
      .mapBoth(errorApplyFunc, _.toBGetOperationStatusResp)

  override def closeOperation(request: BCloseOperationReq, context: RequestContext)
    : IO[StatusException, BCloseOperationResp] =
    driverIO
      .when(
        BitlapContext.isLeader,
        OperationMustOnLeaderException(),
        _.closeOperation(new OperationHandle(request.getOperationHandle))
      )
      .mapBoth(errorApplyFunc, _ => BCloseOperationResp())

  override def getInfo(request: BGetInfoReq, context: RequestContext): IO[StatusException, BGetInfoResp] =
    driverIO
      .when(
        BitlapContext.isLeader,
        OperationMustOnLeaderException(),
        _.getInfo(new SessionHandle(request.getSessionHandle), GetInfoType.toGetInfoType(request.infoType))
      )
      .mapBoth(errorApplyFunc, _.toBGetInfoResp)

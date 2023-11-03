/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

/** RPC server API implementation
 */
object DriverGrpcService:

  lazy val live: ZLayer[AsyncProtocol with ServerNodeContext, Nothing, DriverGrpcService] =
    ZLayer.fromFunction((asyncProtocol: AsyncProtocol, serverNodeContext: ServerNodeContext) =>
      new DriverGrpcService(asyncProtocol, serverNodeContext)
    )

end DriverGrpcService

final class DriverGrpcService(private val asyncProtocol: AsyncProtocol, serverNodeContext: ServerNodeContext)
    extends ZDriverService[RequestContext]:

  // Directly using zio grpc's Status to represent errors and avoid handling multiple errors
  override def openSession(request: BOpenSessionReq, context: RequestContext): IO[StatusException, BOpenSessionResp] =
    asyncProtocol
      .when(
        serverNodeContext.isLeader,
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
    asyncProtocol
      .when(
        serverNodeContext.isLeader,
        OperationMustOnLeaderException(),
        _.closeSession(new SessionHandle(request.getSessionHandle))
      )
      .mapBoth(errorApplyFunc, _ => BCloseSessionResp())

  override def executeStatement(request: BExecuteStatementReq, context: RequestContext)
    : IO[StatusException, BExecuteStatementResp] =
    asyncProtocol
      .when(
        serverNodeContext.isLeader,
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
    asyncProtocol
      .when(
        serverNodeContext.isLeader,
        OperationMustOnLeaderException(),
        _.fetchResults(new OperationHandle(request.getOperationHandle), request.maxRows.toInt, request.fetchType)
      )
      .mapBoth(errorApplyFunc, _.toBFetchResultsResp)

  override def getResultSetMetadata(request: BGetResultSetMetadataReq, context: RequestContext)
    : IO[StatusException, BGetResultSetMetadataResp] =
    asyncProtocol
      .when(
        serverNodeContext.isLeader,
        OperationMustOnLeaderException(),
        _.getResultSetMetadata(new OperationHandle(request.getOperationHandle))
      )
      .mapBoth(errorApplyFunc, _.toBGetResultSetMetadataResp)

  override def getLeader(request: BGetLeaderReq, context: RequestContext): IO[StatusException, BGetLeaderResp] = {
    val leaderAddress = serverNodeContext.getLeaderAddress()
    leaderAddress.flatMap { ld =>
      if ld == null || ld.port <= 0 || ld.ip == null || ld.ip.isEmpty then {
        ZIO.fail(LeaderNotFoundException(s"Invalid ip:port for requestId: ${request.requestId}"))
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
    asyncProtocol
      .when(
        serverNodeContext.isLeader,
        OperationMustOnLeaderException(),
        _.cancelOperation(new OperationHandle(request.getOperationHandle))
      )
      .mapBoth(errorApplyFunc, _ => BCancelOperationResp())

  override def getOperationStatus(request: BGetOperationStatusReq, context: RequestContext)
    : IO[StatusException, BGetOperationStatusResp] =
    asyncProtocol
      .when(
        serverNodeContext.isLeader,
        OperationMustOnLeaderException(),
        _.getOperationStatus(new OperationHandle(request.getOperationHandle))
      )
      .mapBoth(errorApplyFunc, _.toBGetOperationStatusResp)

  override def closeOperation(request: BCloseOperationReq, context: RequestContext)
    : IO[StatusException, BCloseOperationResp] =
    asyncProtocol
      .when(
        serverNodeContext.isLeader,
        OperationMustOnLeaderException(),
        _.closeOperation(new OperationHandle(request.getOperationHandle))
      )
      .mapBoth(errorApplyFunc, _ => BCloseOperationResp())

  override def getInfo(request: BGetInfoReq, context: RequestContext): IO[StatusException, BGetInfoResp] =
    asyncProtocol
      .when(
        serverNodeContext.isLeader,
        OperationMustOnLeaderException(),
        _.getInfo(new SessionHandle(request.getSessionHandle), GetInfoType.toGetInfoType(request.infoType))
      )
      .mapBoth(errorApplyFunc, _.toBGetInfoResp)

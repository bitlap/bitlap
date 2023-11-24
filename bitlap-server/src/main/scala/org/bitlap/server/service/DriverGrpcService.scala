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

import org.bitlap.common.exception.BitlapIllegalArgumentException
import org.bitlap.network.*
import org.bitlap.network.Driver.*
import org.bitlap.network.Driver.ZioDriver.ZDriverService
import org.bitlap.network.enumeration.GetInfoType
import org.bitlap.network.handles.*
import org.bitlap.network.protocol.AsyncProtocol
import org.bitlap.server.*

import io.grpc.*
import scalapb.zio_grpc.RequestContext
import zio.*

/** RPC server API implementation
 */
object DriverGrpcService:

  lazy val live: ZLayer[AsyncProtocol & BitlapGlobalContext, Nothing, DriverGrpcService] =
    ZLayer.fromFunction((asyncProtocol: AsyncProtocol, serverNodeContext: BitlapGlobalContext) =>
      new DriverGrpcService(asyncProtocol, serverNodeContext)
    )

end DriverGrpcService

final class DriverGrpcService(async: AsyncProtocol, serverNodeContext: BitlapGlobalContext)
    extends ZDriverService[RequestContext]:

  // Directly using zio grpc's Status to represent errors and avoid handling multiple errors
  override def openSession(request: BOpenSessionReq, context: RequestContext): IO[StatusException, BOpenSessionResp] =
    async
      .when(
        serverNodeContext.isLeader,
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
    async
      .when(
        serverNodeContext.isLeader,
        _.closeSession(new SessionHandle(request.getSessionHandle))
      )
      .mapBoth(errorApplyFunc, _ => BCloseSessionResp())

  override def executeStatement(request: BExecuteStatementReq, context: RequestContext)
    : IO[StatusException, BExecuteStatementResp] =
    async
      .when(
        serverNodeContext.isLeader,
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
    async
      .when(
        serverNodeContext.isLeader,
        _.fetchResults(new OperationHandle(request.getOperationHandle), request.maxRows.toInt, request.fetchType)
      )
      .mapBoth(errorApplyFunc, _.toBFetchResultsResp)

  override def getResultSetMetadata(request: BGetResultSetMetadataReq, context: RequestContext)
    : IO[StatusException, BGetResultSetMetadataResp] =
    async
      .when(
        serverNodeContext.isLeader,
        _.getResultSetMetadata(new OperationHandle(request.getOperationHandle))
      )
      .mapBoth(errorApplyFunc, _.toBGetResultSetMetadataResp)

  override def getLeader(request: BGetLeaderReq, context: RequestContext): IO[StatusException, BGetLeaderResp] = {
    val leaderAddress = serverNodeContext.getLeaderAddress()
    leaderAddress.flatMap { ld =>
      if ld == null || ld.port <= 0 || ld.ip == null || ld.ip.isEmpty then {
        ZIO.fail(BitlapIllegalArgumentException(s"Invalid ip:port for requestId: ${request.requestId}"))
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
    async
      .when(
        serverNodeContext.isLeader,
        _.cancelOperation(new OperationHandle(request.getOperationHandle))
      )
      .mapBoth(errorApplyFunc, _ => BCancelOperationResp())

  override def getOperationStatus(request: BGetOperationStatusReq, context: RequestContext)
    : IO[StatusException, BGetOperationStatusResp] =
    async
      .when(
        serverNodeContext.isLeader,
        _.getOperationStatus(new OperationHandle(request.getOperationHandle))
      )
      .mapBoth(errorApplyFunc, _.toBGetOperationStatusResp)

  override def closeOperation(request: BCloseOperationReq, context: RequestContext)
    : IO[StatusException, BCloseOperationResp] =
    async
      .when(
        serverNodeContext.isLeader,
        _.closeOperation(new OperationHandle(request.getOperationHandle))
      )
      .mapBoth(errorApplyFunc, _ => BCloseOperationResp())

  override def getInfo(request: BGetInfoReq, context: RequestContext): IO[StatusException, BGetInfoResp] =
    async
      .when(
        serverNodeContext.isLeader,
        _.getInfo(new SessionHandle(request.getSessionHandle), GetInfoType.toGetInfoType(request.infoType))
      )
      .mapBoth(errorApplyFunc, _.toBGetInfoResp)

  override def authenticate(request: BAuthenticateReq, context: RequestContext)
    : IO[StatusException, BAuthenticateResp] = {
    async
      .when(
        serverNodeContext.isLeader,
        _.authenticate(request.username, request.password)
      )
      .mapBoth(errorApplyFunc, _ => BAuthenticateResp())
  }

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
package org.bitlap.client

import org.bitlap.common.utils.StringEx
import org.bitlap.jdbc.BitlapSQLException
import org.bitlap.network.*
import org.bitlap.network.Driver.*
import org.bitlap.network.Driver.ZioDriver.DriverServiceClient
import org.bitlap.network.enumeration.GetInfoType
import org.bitlap.network.enumeration.GetInfoType.toBGetInfoType
import org.bitlap.network.handles.*
import org.bitlap.network.models.*

import io.grpc.*
import zio.*

/** Asynchronous RPC client, implemented based on zio grpc
 */
final class AsyncClient(serverPeers: Array[String], props: Map[String, String]) extends DriverIO:

  /** Based on the configured service cluster, obtain its leader and construct
   *  it[[org.bitlap.network.Driver.ZioDriver.DriverServiceClient]]
   *
   *  Clients use[[org.bitlap.network.Driver.ZioDriver.DriverServiceClient]] to execute SQL, currently, all operations
   *  must be read based on the leader.
   *
   *  TODO (When the leader does not exist, the client cannot perform any operations) cache leader address and add heart
   *  between leader
   */
  private def leaderClientLayer: ZIO[Any, Throwable, Layer[Throwable, DriverServiceClient]] =
    ZIO
      .foreach(serverPeers.asServerAddresses) { address =>
        getLeader(StringEx.uuid(true)).provideLayer(clientLayer(address.ip, address.port))
      }
      .map(_.find(_.isDefined).flatten)
      .map { f =>
        if f.isEmpty then throw BitlapSQLException(s"Cannot find a leader via hosts: ${serverPeers.mkString(",")}")
        clientLayer(f.get.ip, f.get.port)
      }

  /** Obtain grpc channel based on IP:PORT, considering leader transfer, so Layer will be created every time.
   */
  private def clientLayer(ip: String, port: Int): Layer[Throwable, DriverServiceClient] = ZLayer.scoped {
    DriverServiceClient.scoped(
      scalapb.zio_grpc.ZManagedChannel(builder =
        ManagedChannelBuilder.forAddress(ip, port).usePlaintext().asInstanceOf[ManagedChannelBuilder[?]]
      )
    )
  }.orDie

  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String]
  ): ZIO[Any, Throwable, SessionHandle] =
    leaderClientLayer.flatMap(l =>
      DriverServiceClient
        .openSession(BOpenSessionReq(username, password, props ++ configuration))
        .map(r => new SessionHandle(r.getSessionHandle))
        .provideLayer(l)
    )

  override def closeSession(sessionHandle: handles.SessionHandle): ZIO[Any, Throwable, Unit] =
    leaderClientLayer.flatMap(l =>
      DriverServiceClient
        .closeSession(BCloseSessionReq(sessionHandle = Some(sessionHandle.toBSessionHandle())))
        .unit
        .provideLayer(l)
    )

  override def executeStatement(
    sessionHandle: handles.SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String]
  ): ZIO[Any, Throwable, OperationHandle] =
    leaderClientLayer.flatMap(l =>
      DriverServiceClient
        .executeStatement(
          BExecuteStatementReq(statement, Some(sessionHandle.toBSessionHandle()), props ++ confOverlay, queryTimeout)
        )
        .map(r => new OperationHandle(r.getOperationHandle))
        .provideLayer(l)
    )

  override def fetchResults(
    opHandle: OperationHandle,
    maxRows: Int = 50,
    fetchType: Int = 1
  ): ZIO[Any, Throwable, FetchResults] =
    leaderClientLayer.flatMap(l =>
      DriverServiceClient
        .fetchResults(
          BFetchResultsReq(Some(opHandle.toBOperationHandle()), maxRows, fetchType)
        )
        .map(r => FetchResults.fromBFetchResultsResp(r))
        .provideLayer(l)
    )

  override def getResultSetMetadata(opHandle: OperationHandle): ZIO[Any, Throwable, TableSchema] =
    leaderClientLayer.flatMap(l =>
      DriverServiceClient
        .getResultSetMetadata(BGetResultSetMetadataReq(Some(opHandle.toBOperationHandle())))
        .map(t => TableSchema.fromBGetResultSetMetadataResp(t))
        .provideLayer(l)
    )

  private[client] def getLeader(requestId: String): ZIO[DriverServiceClient, Nothing, Option[ServerAddress]] =
    DriverServiceClient
      .getLeader(BGetLeaderReq.of(requestId))
      .map { f =>
        if f == null || f.ip.isEmpty then None else Some(ServerAddress(f.ip.getOrElse("localhost"), f.port))
      }
      .catchSomeCause {
        case c if c.contains(Cause.fail(Status.ABORTED)) => ZIO.succeed(Option.empty[ServerAddress]) // ignore this
      }
      .catchAll(_ => ZIO.none)

  override def cancelOperation(opHandle: OperationHandle): Task[Unit] =
    leaderClientLayer.flatMap(l =>
      DriverServiceClient
        .cancelOperation(BCancelOperationReq(Option(opHandle).map(_.toBOperationHandle())))
        .unit
        .provideLayer(l)
    )

  override def getOperationStatus(opHandle: OperationHandle): Task[OperationStatus] =
    leaderClientLayer.flatMap(l =>
      DriverServiceClient
        .getOperationStatus(BGetOperationStatusReq(Option(opHandle).map(_.toBOperationHandle())))
        .map(t => OperationStatus.fromBGetOperationStatusResp(t))
        .provideLayer(l)
    )

  override def closeOperation(opHandle: OperationHandle): Task[Unit] =
    leaderClientLayer.flatMap(l =>
      DriverServiceClient
        .closeOperation(BCloseOperationReq(Option(opHandle).map(_.toBOperationHandle())))
        .unit
        .provideLayer(l)
    )

  override def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): Task[GetInfoValue] =
    leaderClientLayer.flatMap(l =>
      DriverServiceClient
        .getInfo(BGetInfoReq(Option(sessionHandle.toBSessionHandle()), toBGetInfoType(getInfoType)))
        .map(t => GetInfoValue.fromBGetInfoResp(t))
        .provideLayer(l)
    )

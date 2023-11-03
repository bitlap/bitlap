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
final class AsyncClient(serverPeers: Array[String], props: Map[String, String]) extends AsyncProtocol:

  assert(serverPeers.length > 0)

  private val addr = serverPeers.asServerAddresses

  private val leaderRef: UIO[Ref[ZIO[Scope, Throwable, DriverServiceClient]]] =
    Ref.make(clientLayer(addr.head.ip, addr.head.port))

  /** Based on the configured service cluster, obtain its leader and construct
   *  it[[org.bitlap.network.Driver.ZioDriver.DriverServiceClient]]
   *
   *  Clients use[[org.bitlap.network.Driver.ZioDriver.DriverServiceClient]] to execute SQL, currently, all operations
   *  must be read based on the leader.
   *
   *  TODO (When the leader does not exist, the client cannot perform any operations)
   */
  private def leaderClientLayer: ZLayer[Any, Throwable, DriverServiceClient] = ZLayer.scoped {
    for {
      c <- ZIO.foreach(serverPeers.asServerAddresses) { address =>
        val layer = clientLayer(address.ip, address.port)
        leaderRef.flatMap { ref =>
          getLeader(StringEx.uuid(true)).provideLayer(ZLayer.scoped((layer))).onExit {
            case Exit.Success(value) if value.isDefined =>
              ref.set(layer)
            case _ =>
              ZIO.unit
          }
        }
      }
      live <- ZIO
        .when(c.flatMap(_.toList).nonEmpty) {
          leaderRef.flatMap(_.get)
        }
        .someOrFail(BitlapSQLException(s"Cannot find a leader via hosts: ${serverPeers.mkString(",")}"))
      client <- live
    } yield client

  }

  /** Obtain grpc channel based on IP:PORT, considering leader transfer, so Layer will be created every time.
   */
  private def clientLayer(ip: String, port: Int): ZIO[Scope, Throwable, DriverServiceClient] =
    DriverServiceClient.scoped(
      scalapb.zio_grpc.ZManagedChannel(builder =
        ManagedChannelBuilder.forAddress(ip, port).usePlaintext().asInstanceOf[ManagedChannelBuilder[?]]
      )
    )

  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String]
  ): ZIO[Any, Throwable, SessionHandle] =
    DriverServiceClient
      .openSession(BOpenSessionReq(username, password, props ++ configuration))
      .map(r => new SessionHandle(r.getSessionHandle))
      .provideLayer(leaderClientLayer)

  override def closeSession(sessionHandle: handles.SessionHandle): ZIO[Any, Throwable, Unit] =
    DriverServiceClient
      .closeSession(BCloseSessionReq(sessionHandle = Some(sessionHandle.toBSessionHandle())))
      .unit
      .provideLayer(leaderClientLayer)

  override def executeStatement(
    sessionHandle: handles.SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String]
  ): ZIO[Any, Throwable, OperationHandle] =
    DriverServiceClient
      .executeStatement(
        BExecuteStatementReq(statement, Some(sessionHandle.toBSessionHandle()), props ++ confOverlay, queryTimeout)
      )
      .map(r => new OperationHandle(r.getOperationHandle))
      .provideLayer(leaderClientLayer)

  override def fetchResults(
    opHandle: OperationHandle,
    maxRows: Int = 50,
    fetchType: Int = 1
  ): ZIO[Any, Throwable, FetchResults] =
    DriverServiceClient
      .fetchResults(
        BFetchResultsReq(Some(opHandle.toBOperationHandle()), maxRows, fetchType)
      )
      .map(r => FetchResults.fromBFetchResultsResp(r))
      .provideLayer(leaderClientLayer)

  override def getResultSetMetadata(opHandle: OperationHandle): ZIO[Any, Throwable, TableSchema] =
    DriverServiceClient
      .getResultSetMetadata(BGetResultSetMetadataReq(Some(opHandle.toBOperationHandle())))
      .map(t => TableSchema.fromBGetResultSetMetadataResp(t))
      .provideLayer(leaderClientLayer)

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
    DriverServiceClient
      .cancelOperation(BCancelOperationReq(Option(opHandle).map(_.toBOperationHandle())))
      .unit
      .provideLayer(leaderClientLayer)

  override def getOperationStatus(opHandle: OperationHandle): Task[OperationStatus] =
    DriverServiceClient
      .getOperationStatus(BGetOperationStatusReq(Option(opHandle).map(_.toBOperationHandle())))
      .map(t => OperationStatus.fromBGetOperationStatusResp(t))
      .provideLayer(leaderClientLayer)

  override def closeOperation(opHandle: OperationHandle): Task[Unit] =
    DriverServiceClient
      .closeOperation(BCloseOperationReq(Option(opHandle).map(_.toBOperationHandle())))
      .unit
      .provideLayer(leaderClientLayer)

  override def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): Task[GetInfoValue] =
    DriverServiceClient
      .getInfo(BGetInfoReq(Option(sessionHandle.toBSessionHandle()), toBGetInfoType(getInfoType)))
      .map(t => GetInfoValue.fromBGetInfoResp(t))
      .provideLayer(leaderClientLayer)

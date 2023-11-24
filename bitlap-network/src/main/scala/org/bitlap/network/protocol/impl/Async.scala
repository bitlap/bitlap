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
package org.bitlap.network.protocol.impl

import java.util.concurrent.atomic.AtomicReference

import org.bitlap.common.exception.BitlapException
import org.bitlap.common.exception.BitlapIllegalStateException
import org.bitlap.common.utils.StringEx
import org.bitlap.network.*
import org.bitlap.network.Driver.*
import org.bitlap.network.Driver.ZioDriver.DriverServiceClient
import org.bitlap.network.enumeration.GetInfoType
import org.bitlap.network.enumeration.GetInfoType.toBGetInfoType
import org.bitlap.network.handles.*
import org.bitlap.network.models.*
import org.bitlap.network.protocol.*

import io.grpc.*
import zio.*

/** Asynchronous RPC client, implemented based on zio grpc
 */
final class Async(serverPeers: Array[String], props: Map[String, String]) extends AsyncProtocol:

  assert(serverPeers.length > 0)

  private val addr = serverPeers.asServerAddresses

  private val addRef: AtomicReference[ServerAddress] =
    new AtomicReference[ServerAddress](null)

  /** Based on the configured service cluster, obtain its leader and construct
   *  it[[org.bitlap.network.Driver.ZioDriver.DriverServiceClient]]
   *
   *  Clients use[[org.bitlap.network.Driver.ZioDriver.DriverServiceClient]] to execute SQL, currently, all operations
   *  must be read based on the leader.
   */
  private def leaderClientLayer: ZLayer[Any, Throwable, DriverServiceClient] =
    ZLayer.make[DriverServiceClient](
      Scope.default,
      ZLayer.fromZIO(
        for {
          leaderLayer <-
            if (addRef.get() == null) {
              clientLayer(addr.head.ip, addr.head.port)
                .flatMap(leaderLayer =>
                  leaderLayer
                    .getLeader(BGetLeaderReq.of(StringEx.uuid(true)))
                    .map(f => if f.ip.isEmpty then None else Some(ServerAddress(f.ip.getOrElse("127.0.0.1"), f.port)))
                    .someOrFail(BitlapException(s"Cannot find a leader via hosts: ${serverPeers.mkString(",")}"))
                )
                .flatMap(address => clientLayer(address.ip, address.port) <* ZIO.succeed(addRef.set(address)))
            } else clientLayer(addRef.get().ip, addRef.get().port)
        } yield leaderLayer
      )
    )

  /** Get grpc channel based on IP:PORT.
   */
  private def clientLayer(ip: String, port: Int): ZIO[Scope, Throwable, DriverServiceClient] =
    DriverServiceClient.scoped(
      scalapb.zio_grpc.ZManagedChannel(builder =
        ManagedChannelBuilder.forAddress(ip, port).usePlaintext().asInstanceOf[ManagedChannelBuilder[?]]
      )
    )

  private inline def onErrorFunc(cleanup: Cause[Throwable]): UIO[Unit] = {
    cleanup match
      case Cause.Fail(value, trace) =>
        value match {
          case state: BitlapIllegalStateException =>
            addRef.set(null)
            ZIO.unit
          case _ => ZIO.unit
        }
      case _ => ZIO.unit
  }

  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String]
  ): ZIO[Any, Throwable, SessionHandle] =
    DriverServiceClient
      .openSession(BOpenSessionReq(username, password, props ++ configuration))
      .map(r => new SessionHandle(r.getSessionHandle))
      .provideLayer(leaderClientLayer)
      .onError(e => onErrorFunc(e))

  override def closeSession(sessionHandle: handles.SessionHandle): ZIO[Any, Throwable, Unit] =
    DriverServiceClient
      .closeSession(BCloseSessionReq(sessionHandle = Some(sessionHandle.toBSessionHandle())))
      .unit
      .provideLayer(leaderClientLayer)
      .onError(e => onErrorFunc(e))

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
      .onError(e => onErrorFunc(e))

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
      .onError(e => onErrorFunc(e))

  override def getResultSetMetadata(opHandle: OperationHandle): ZIO[Any, Throwable, TableSchema] =
    DriverServiceClient
      .getResultSetMetadata(BGetResultSetMetadataReq(Some(opHandle.toBOperationHandle())))
      .map(t => TableSchema.fromBGetResultSetMetadataResp(t))
      .provideLayer(leaderClientLayer)
      .onError(e => onErrorFunc(e))

  override def cancelOperation(opHandle: OperationHandle): Task[Unit] =
    DriverServiceClient
      .cancelOperation(BCancelOperationReq(Option(opHandle).map(_.toBOperationHandle())))
      .unit
      .provideLayer(leaderClientLayer)
      .onError(e => onErrorFunc(e))

  override def getOperationStatus(opHandle: OperationHandle): Task[OperationStatus] =
    DriverServiceClient
      .getOperationStatus(BGetOperationStatusReq(Option(opHandle).map(_.toBOperationHandle())))
      .map(t => OperationStatus.fromBGetOperationStatusResp(t))
      .provideLayer(leaderClientLayer)
      .onError(e => onErrorFunc(e))

  override def closeOperation(opHandle: OperationHandle): Task[Unit] =
    DriverServiceClient
      .closeOperation(BCloseOperationReq(Option(opHandle).map(_.toBOperationHandle())))
      .unit
      .provideLayer(leaderClientLayer)
      .onError(e => onErrorFunc(e))

  override def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): Task[GetInfoValue] =
    DriverServiceClient
      .getInfo(BGetInfoReq(Option(sessionHandle.toBSessionHandle()), toBGetInfoType(getInfoType)))
      .map(t => GetInfoValue.fromBGetInfoResp(t))
      .provideLayer(leaderClientLayer)
      .onError(e => onErrorFunc(e))

object Async {

  def make(conf: ClientConfig): ULayer[Async] = ZLayer.succeed(
    new Async(conf.serverPeers.toArray, conf.props)
  )
}

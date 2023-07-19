/* Copyright (c) 2023 bitlap.org */
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

/** 异步RPC客户端，基于zio-grpc实现
 *
 *  @author
 *    梦境迷离
 *  @since 2021/11/21
 *  @version 1.0, zio 2.0
 */
final class AsyncClient(serverPeers: Array[String], props: Map[String, String]) extends DriverIO:

  /** 根据配置的服务集群，获取其leader，构造[[org.bitlap.network.Driver.ZioDriver.DriverServiceClient]]
   *
   *  客户端使用[[org.bitlap.network.Driver.ZioDriver.DriverServiceClient]]操作SQL，目前所有操作必须读基于leader。
   *
   *  TODO (当leader不存在时，client无法做任何操作)
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

  /** 根据 IP:PORT 获取grpc channel，考虑leader转移，所以每次都将创建Layer
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

  override def getDatabases(sessionHandle: SessionHandle, pattern: String): ZIO[Any, Throwable, OperationHandle] =
    leaderClientLayer.flatMap(l =>
      DriverServiceClient
        .getDatabases(BGetDatabasesReq(Option(sessionHandle.toBSessionHandle()), pattern))
        .map(t => new OperationHandle(t.getOperationHandle))
        .provideLayer(l)
    )

  override def getTables(
    sessionHandle: SessionHandle,
    database: String,
    pattern: String
  ): ZIO[Any, Throwable, OperationHandle] =
    leaderClientLayer.flatMap(l =>
      DriverServiceClient
        .getTables(BGetTablesReq(Option(sessionHandle.toBSessionHandle()), database, pattern))
        .map(t => new OperationHandle(t.getOperationHandle))
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

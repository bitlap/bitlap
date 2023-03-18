/* Copyright (c) 2023 bitlap.org */
package org.bitlap.client

import io.grpc._
import org.bitlap.common.utils.UuidUtil
import org.bitlap.jdbc.BitlapSQLException
import org.bitlap.network._
import org.bitlap.network.driver_proto._
import org.bitlap.network.driver_service.ZioDriverService.DriverServiceClient
import org.bitlap.network.enumeration.GetInfoType
import org.bitlap.network.enumeration.GetInfoType.toBGetInfoType
import org.bitlap.network.handles._
import org.bitlap.network.models._
import zio._

/** 异步RPC客户端，基于zio-grpc实现
 *
 *  @author
 *    梦境迷离
 *  @since 2021/11/21
 *  @version 1.0, zio 1.0
 */
final class AsyncClient(serverPeers: Array[String], props: Map[String, String]) extends DriverAsyncRpc {

  private lazy val leaderClientLayer = ZIO
    .foreach(toServerAddresses(serverPeers)) { address =>
      getLeader(UuidUtil.uuid()).provideLayer(clientLayer(address.ip, address.port))
    }
    .map(f =>
      f.collectFirst { case Some(value) =>
        value
      }
    )
    .map(l =>
      if (l.isDefined) l.get
      else throw BitlapSQLException(s"Cannot find a leader by hosts: ${serverPeers.mkString(",")}")
    )
    .map(f => clientLayer(f.ip, f.port))

  private def clientLayer(ip: String, port: Int): Layer[Throwable, DriverServiceClient] = DriverServiceClient.live(
    scalapb.zio_grpc.ZManagedChannel(builder =
      ManagedChannelBuilder.forAddress(ip, port).usePlaintext().asInstanceOf[ManagedChannelBuilder[_]]
    )
  )

  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String]
  ): ZIO[Any, Throwable, SessionHandle] =
    leaderClientLayer.flatMap(l =>
      DriverServiceClient
        .openSession(BOpenSessionReq(username, password, props ++ configuration))
        .mapBoth(statusApplyFunc, r => new SessionHandle(r.getSessionHandle))
        .provideLayer(l)
    )

  override def closeSession(sessionHandle: handles.SessionHandle): ZIO[Any, Throwable, Unit] =
    leaderClientLayer.flatMap(l =>
      DriverServiceClient
        .closeSession(BCloseSessionReq(sessionHandle = Some(sessionHandle.toBSessionHandle())))
        .as()
        .mapError(statusApplyFunc)
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
        .mapBoth(statusApplyFunc, r => new OperationHandle(r.getOperationHandle))
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
        .mapBoth(statusApplyFunc, r => FetchResults.fromBFetchResultsResp(r))
        .provideLayer(l)
    )

  override def getResultSetMetadata(opHandle: OperationHandle): ZIO[Any, Throwable, TableSchema] =
    leaderClientLayer.flatMap(l =>
      DriverServiceClient
        .getResultSetMetadata(BGetResultSetMetadataReq(Some(opHandle.toBOperationHandle())))
        .mapBoth(statusApplyFunc, t => TableSchema.fromBTableSchema(t.getSchema))
        .provideLayer(l)
    )

  override def getDatabases(sessionHandle: SessionHandle, pattern: String): ZIO[Any, Throwable, OperationHandle] =
    leaderClientLayer.flatMap(l =>
      DriverServiceClient
        .getDatabases(BGetDatabasesReq(Option(sessionHandle.toBSessionHandle()), pattern))
        .mapBoth(statusApplyFunc, t => new OperationHandle(t.getOperationHandle))
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
        .mapBoth(statusApplyFunc, t => new OperationHandle(t.getOperationHandle))
        .provideLayer(l)
    )

  private[client] def getLeader(requestId: String): ZIO[DriverServiceClient, Nothing, Option[ServerAddress]] =
    DriverServiceClient
      .getLeader(BGetLeaderReq.of(requestId))
      .map { f =>
        if (f == null || f.ip.isEmpty) None else Some(ServerAddress(f.ip.getOrElse("localhost"), f.port))
      }
      .catchSomeCause {
        case c if c.contains(Cause.fail(Status.ABORTED)) => ZIO.succeed(Option.empty[ServerAddress]) // ignore this
      }
      .catchAll(_ => ZIO.none)

  override def cancelOperation(opHandle: OperationHandle): Task[Unit] =
    leaderClientLayer.flatMap(l =>
      DriverServiceClient
        .cancelOperation(BCancelOperationReq(Option(opHandle).map(_.toBOperationHandle())))
        .mapBoth(statusApplyFunc, _ => ())
        .provideLayer(l)
    )

  override def getOperationStatus(opHandle: OperationHandle): Task[OperationStatus] =
    leaderClientLayer.flatMap(l =>
      DriverServiceClient
        .getOperationStatus(BGetOperationStatusReq(Option(opHandle).map(_.toBOperationHandle())))
        .mapBoth(statusApplyFunc, t => OperationStatus.fromBGetOperationStatusResp(t))
        .provideLayer(l)
    )

  override def closeOperation(opHandle: OperationHandle): Task[Unit] =
    leaderClientLayer.flatMap(l =>
      DriverServiceClient
        .closeOperation(BCloseOperationReq(Option(opHandle).map(_.toBOperationHandle())))
        .mapBoth(statusApplyFunc, _ => ())
        .provideLayer(l)
    )

  override def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): Task[GetInfoValue] =
    leaderClientLayer.flatMap(l =>
      DriverServiceClient
        .getInfo(BGetInfoReq(Option(sessionHandle.toBSessionHandle()), toBGetInfoType(getInfoType)))
        .mapBoth(statusApplyFunc, t => GetInfoValue.fromBGetInfoResp(t))
        .provideLayer(l)
    )
}

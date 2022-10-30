/* Copyright (c) 2022 bitlap.org */
package org.bitlap.client

import io.grpc.{ ManagedChannelBuilder, Status }
import org.bitlap.jdbc.BSQLException
import org.bitlap.network._
import org.bitlap.network.driver.proto.BCloseSession.BCloseSessionReq
import org.bitlap.network.driver.proto.BExecuteStatement.BExecuteStatementReq
import org.bitlap.network.driver.proto.BFetchResults.BFetchResultsReq
import org.bitlap.network.driver.proto.BGetRaftMetadata
import org.bitlap.network.driver.proto.BGetResultSetMetadata.BGetResultSetMetadataReq
import org.bitlap.network.driver.proto.BOpenSession.BOpenSessionReq
import org.bitlap.network.driver.service.ZioService.DriverServiceClient
import org.bitlap.network.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.models.{ FetchResults, TableSchema }
import zio._

import java.util.UUID

/** This class mainly wraps zio rpc calling procedures.
 *
 *  @author
 *    梦境迷离
 *  @since 2021/11/21
 *  @version 1.0
 */
class BitlapAsyncClient(uri: String, port: Int, serverPeers: Array[String], props: Map[String, String])
    extends AsyncRpc
    with RpcStatus {

  // refactor
  private lazy val serverAddresses =
    serverPeers
      .filter(_.contains(":"))
      .map(s => LeaderAddress(s.split(":")(0).trim, s.split(":")(1).trim.toIntOption.getOrElse(23333)))
      .toList

  private val leaderAddress = ZIO
    .foreach(serverAddresses) { address =>
      getLeader(UUID.randomUUID().toString.replaceAll("-", "")).provideLayer(clientLayer(address.ip, address.port))
    }
    .map(f =>
      f.collectFirst { case Some(value) =>
        value
      }
    )
    .filterOrDie(_.exists(_ != null))(BSQLException("Leader not found"))
    .map(_.get)

  private lazy val leaderClientLayer = leaderAddress.map(f => clientLayer(f.ip, f.port))

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
        .openSession(BOpenSessionReq(username, password, configuration))
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
          BExecuteStatementReq(statement, Some(sessionHandle.toBSessionHandle()), confOverlay, queryTimeout)
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

  override def getColumns(
    sessionHandle: SessionHandle,
    tableName: String,
    schemaName: String,
    columnName: String
  ): ZIO[Any, Throwable, OperationHandle] = ???

  override def getDatabases(pattern: String): ZIO[Any, Throwable, OperationHandle] = ???

  override def getTables(database: String, pattern: String): ZIO[Any, Throwable, OperationHandle] = ???

  override def getSchemas(
    sessionHandle: SessionHandle,
    catalogName: String,
    schemaName: String
  ): ZIO[Any, Throwable, OperationHandle] = ???

  private[client] def getLeader(requestId: String): ZIO[DriverServiceClient, Throwable, Option[LeaderAddress]] =
    DriverServiceClient
      .getLeader(BGetRaftMetadata.BGetLeaderReq.of(requestId))
      .map(f => Some(LeaderAddress(f.ip, f.port)))
      .catchSomeCause {
        case c if c.contains(Cause.fail(Status.ABORTED)) => ZIO.succeed(Option.empty[LeaderAddress]) // ignore this
      }
      .mapError(statusApplyFunc)
}

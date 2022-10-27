/* Copyright (c) 2022 bitlap.org */
package org.bitlap.client

import io.grpc.ManagedChannelBuilder
import org.bitlap.network._
import org.bitlap.network.driver.proto.BCloseSession.BCloseSessionReq
import org.bitlap.network.driver.proto.BExecuteStatement.BExecuteStatementReq
import org.bitlap.network.driver.proto.BFetchResults.BFetchResultsReq
import org.bitlap.network.driver.proto.BGetResultSetMetadata.BGetResultSetMetadataReq
import org.bitlap.network.driver.proto.BOpenSession.BOpenSessionReq
import org.bitlap.network.driver.service.ZioService.DriverServiceClient
import org.bitlap.network.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.models.{ FetchResults, TableSchema }
import zio._

/** This class mainly wraps zio rpc calling procedures.
 *
 *  @author
 *    梦境迷离
 *  @since 2021/11/21
 *  @version 1.0
 */
class BitlapAsyncClient(uri: String, port: Int, props: Map[String, String]) extends AsyncRpc with RpcStatus {

  private val clientLayer: Layer[Throwable, DriverServiceClient] = DriverServiceClient.live(
    scalapb.zio_grpc.ZManagedChannel(ManagedChannelBuilder.forAddress(uri, port).usePlaintext())
  )

  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String]
  ): ZIO[Any, Throwable, SessionHandle] =
    DriverServiceClient
      .openSession(BOpenSessionReq(username, password, configuration))
      .mapBoth(statusApplyFunc, r => new SessionHandle(r.getSessionHandle))
      .provideLayer(clientLayer)

  override def closeSession(sessionHandle: handles.SessionHandle): ZIO[Any, Throwable, Unit] =
    DriverServiceClient
      .closeSession(BCloseSessionReq(sessionHandle = Some(sessionHandle.toBSessionHandle())))
      .as()
      .mapError(statusApplyFunc)
      .provideLayer(clientLayer)

  override def executeStatement(
    sessionHandle: handles.SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String]
  ): ZIO[Any, Throwable, OperationHandle] =
    DriverServiceClient
      .executeStatement(
        BExecuteStatementReq(statement, Some(sessionHandle.toBSessionHandle()), confOverlay, queryTimeout)
      )
      .mapBoth(statusApplyFunc, r => new OperationHandle(r.getOperationHandle))
      .provideLayer(clientLayer)

  override def fetchResults(
    opHandle: OperationHandle,
    maxRows: Int = 50,
    fetchType: Int = 1
  ): ZIO[Any, Throwable, FetchResults] =
    DriverServiceClient
      .fetchResults(
        BFetchResultsReq(Some(opHandle.toBOperationHandle()), maxRows, fetchType)
      )
      .mapBoth(statusApplyFunc, r => FetchResults.fromBFetchResultsResp(r))
      .provideLayer(clientLayer)

  override def getResultSetMetadata(opHandle: OperationHandle): ZIO[Any, Throwable, TableSchema] =
    DriverServiceClient
      .getResultSetMetadata(BGetResultSetMetadataReq(Some(opHandle.toBOperationHandle())))
      .mapBoth(statusApplyFunc, t => TableSchema.fromBTableSchema(t.getSchema))
      .provideLayer(clientLayer)

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
}

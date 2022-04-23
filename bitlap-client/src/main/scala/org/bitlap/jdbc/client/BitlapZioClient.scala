/* Copyright (c) 2022 bitlap.org */
package org.bitlap.jdbc.client

import io.grpc.ManagedChannelBuilder
import org.bitlap.common.BitlapConf
import org.bitlap.network.driver.proto.BCloseSession.BCloseSessionReq
import org.bitlap.network.driver.proto.BExecuteStatement.BExecuteStatementReq
import org.bitlap.network.driver.proto.BFetchResults.BFetchResultsReq
import org.bitlap.network.driver.proto.BGetResultSetMetadata.{ BGetResultSetMetadataReq, BGetResultSetMetadataResp }
import org.bitlap.network.driver.proto.BOpenSession.BOpenSessionReq
import org.bitlap.network.driver.service.ZioService.DriverServiceClient
import org.bitlap.network.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.models.{ FetchResults, TableSchema }
import org.bitlap.network.rpc.{ exception, RpcN }
import org.bitlap.network.{ handles, models, RpcStatus }
import scalapb.zio_grpc.ZManagedChannel
import zio.{ Layer, ZIO }

import scala.jdk.CollectionConverters._

/**
 * This class mainly wraps the RPC call procedure used inside JDBC.
 *
 * @author 梦境迷离
 * @since 2021/11/21
 * @version 1.0
 */
private[jdbc] class BitlapZioClient(uri: String, port: Int, props: Map[String, String])
    extends RpcN[ZIO]
    with RpcStatus {

  private lazy val conf: BitlapConf = new BitlapConf(props.asJava)
  val readTimeout: java.lang.Long = conf.get(BitlapConf.NODE_READ_TIMEOUT)

  val maxRows = 50
  val fetchType = 1

  private val clientLayer: Layer[Throwable, DriverServiceClient] = DriverServiceClient.live(
    ZManagedChannel(ManagedChannelBuilder.forAddress(uri, port).usePlaintext())
  )

  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String]
  ): ZIO[Any, Throwable, SessionHandle] =
    DriverServiceClient
      .openSession(BOpenSessionReq(username, password, configuration))
      .map(r => new SessionHandle(r.getSessionHandle)) // 因为server和client使用一套API。必须转换以下
      .mapError(f => new Throwable(f.asException()))
      .provideLayer(clientLayer)

  override def closeSession(sessionHandle: handles.SessionHandle): ZIO[Any, Throwable, Unit] =
    DriverServiceClient
      .closeSession(BCloseSessionReq(sessionHandle = Some(sessionHandle.toBSessionHandle())))
      .map(_ => ())
      .mapError(st => new Throwable(exception(st)))
      .provideLayer(clientLayer)

  override def executeStatement(
    sessionHandle: handles.SessionHandle,
    statement: String,
    queryTimeout: Long = readTimeout,
    confOverlay: Map[String, String]
  ): ZIO[Any, Throwable, OperationHandle] =
    DriverServiceClient
      .executeStatement(
        BExecuteStatementReq(statement, Some(sessionHandle.toBSessionHandle()), confOverlay, queryTimeout)
      )
      .map(r => new OperationHandle(r.getOperationHandle))
      .mapError(st => new Throwable(exception(st)))
      .provideLayer(clientLayer)

  override def fetchResults(opHandle: OperationHandle): ZIO[Any, Throwable, FetchResults] =
    DriverServiceClient
      .fetchResults(
        BFetchResultsReq(Some(opHandle.toBOperationHandle()), maxRows, fetchType)
      )
      .map(r => FetchResults.fromBFetchResultsResp(r))
      .mapError(st => new Throwable(exception(st)))
      .provideLayer(clientLayer)

  override def getResultSetMetadata(opHandle: OperationHandle): ZIO[Any, Throwable, TableSchema] =
    DriverServiceClient
      .getResultSetMetadata(BGetResultSetMetadataReq(Some(opHandle.toBOperationHandle())))
      .map(t => TableSchema.fromBTableSchema(t.getSchema))
      .mapError(st => new Throwable(exception(st)))
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

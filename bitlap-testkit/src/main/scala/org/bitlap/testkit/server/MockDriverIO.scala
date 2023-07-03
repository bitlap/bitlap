/* Copyright (c) 2023 bitlap.org */
package org.bitlap.testkit.server

import org.bitlap.network.*
import org.bitlap.network.enumeration.*
import org.bitlap.network.handles.*
import org.bitlap.network.models.*
import org.bitlap.network.serde.BitlapSerde
import org.bitlap.testkit.*

import com.google.protobuf.ByteString

import zio.*

/** 用于测试的 bitlap rpc 服务端实现
 *
 *  使用固定的测试数据: simple_data.csv
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/27
 */
object MockDriverIO {
  lazy val live: ULayer[DriverIO] = ZLayer.succeed(new MockDriverIO())
}

final class MockDriverIO extends DriverIO with CSVUtils {

  val metrics: Seq[Metric] = readClasspathCSVData("simple_data.csv")

  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String] = Map.empty
  ): ZIO[Any, Throwable, SessionHandle] = ZIO.succeed(SessionHandle(HandleIdentifier()))

  override def closeSession(sessionHandle: SessionHandle): ZIO[Any, Throwable, Unit] = ZIO.unit

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String] = Map.empty
  ): ZIO[Any, Throwable, OperationHandle] =
    ZIO.succeed(new OperationHandle(OperationType.ExecuteStatement, true, sessionHandle.handleId))

  override def fetchResults(
    opHandle: OperationHandle,
    maxRows: Int,
    fetchType: Int
  ): ZIO[Any, Throwable, FetchResults] = {
    val convert = (metric: Metric) =>
      List(
        BitlapSerde.serialize(metric.time),
        BitlapSerde.serialize(metric.entity),
        BitlapSerde.serialize(metric.dimensions.map(_.value).headOption.getOrElse("")),
        BitlapSerde.serialize(metric.name),
        BitlapSerde.serialize(metric.value)
      )
    ZIO.succeed(
      FetchResults(
        hasMoreRows = false,
        RowSet(
          metrics.toList.map(m => Row(convert(m)))
        )
      )
    )
  }

  override def getResultSetMetadata(opHandle: OperationHandle): ZIO[Any, Throwable, TableSchema] =
    ZIO.succeed(
      TableSchema.apply(
        List(
          ColumnDesc("time", TypeId.LongType),
          ColumnDesc("entity", TypeId.IntType),
          ColumnDesc("dimensions", TypeId.StringType), // TODO not support object type
          ColumnDesc("metric_name", TypeId.StringType),
          ColumnDesc("metric_value", TypeId.IntType)
        )
      )
    )

  override def getDatabases(sessionHandle: SessionHandle, pattern: String): ZIO[Any, Throwable, OperationHandle] =
    ZIO.succeed(new OperationHandle(OperationType.GetSchemas))

  override def getTables(
    sessionHandle: SessionHandle,
    database: String,
    pattern: String
  ): ZIO[Any, Throwable, OperationHandle] =
    ZIO.succeed(new OperationHandle(OperationType.GetTables))

  override def cancelOperation(opHandle: OperationHandle): Task[Unit] = ZIO.unit

  override def closeOperation(opHandle: OperationHandle): Task[Unit] = ZIO.unit

  override def getOperationStatus(opHandle: OperationHandle): Task[OperationStatus] =
    ZIO.succeed(OperationStatus(Some(true), Some(OperationState.FinishedState)))

  override def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): Task[GetInfoValue] = ???
}

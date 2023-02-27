/* Copyright (c) 2023 bitlap.org */
package org.bitlap.testkit.server

import com.google.protobuf.ByteString
import org.bitlap.network._
import org.bitlap.network.handles._
import org.bitlap.network.models._
import org.bitlap.testkit._
import org.bitlap.tools.apply
import zio._

/** 用于测试的 bitlap rpc 服务端实现
 *
 *  使用固定的测试数据：simple_data.csv
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/27
 */
@apply
class MockAsyncRpcBackend extends AsyncRpc with CsvUtil {

  val metrics: Seq[Metric] = readCsvData("simple_data.csv")

  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String] = Map.empty
  ): ZIO[Any, Throwable, SessionHandle] = ZIO.succeed(SessionHandle(new HandleIdentifier()))

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
        ByteString.copyFromUtf8(metric.time.toString),
        ByteString.copyFromUtf8(metric.entity.toString),
        ByteString.copyFromUtf8(metric.dimensions.map(_.value).headOption.getOrElse("")),
        ByteString.copyFromUtf8(metric.name),
        ByteString.copyFromUtf8(metric.value.toString)
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
          ColumnDesc("time", TypeId.IntType),
          ColumnDesc("entity", TypeId.LongType),
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

  override def cancelOperation(opHandle: OperationHandle): Task[Unit] = Task.unit

  override def closeOperation(opHandle: OperationHandle): Task[Unit] = Task.unit

  override def getOperationStatus(opHandle: OperationHandle): Task[OperationStatus] =
    Task.succeed(OperationStatus(Some(true), Some(OperationState.FinishedState)))
}

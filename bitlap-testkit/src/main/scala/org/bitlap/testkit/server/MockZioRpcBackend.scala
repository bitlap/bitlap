/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit.server

import com.google.protobuf.ByteString
import org.bitlap.network.{ OperationType, RpcZio }
import org.bitlap.network.handles.{ HandleIdentifier, OperationHandle, SessionHandle }
import org.bitlap.network.models._
import org.bitlap.testkit.{ CsvUtil, Metric }
import org.bitlap.tools.apply
import zio.ZIO

/** Mock backend for rpc implementation.
 *
 *  @author
 *    梦境迷离
 *  @version 1.0,2022/4/27
 */
@apply
class MockZioRpcBackend extends RpcZio with CsvUtil {

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

  override def getColumns(
    sessionHandle: SessionHandle,
    schemaName: String,
    tableName: String,
    columnName: String
  ): ZIO[Any, Throwable, OperationHandle] = ZIO.effect(new OperationHandle(OperationType.GetColumns))

  override def getDatabases(pattern: String): ZIO[Any, Throwable, OperationHandle] =
    ZIO.succeed(new OperationHandle(OperationType.GetSchemas)) // TODO add rpc method ??

  override def getTables(database: String, pattern: String): ZIO[Any, Throwable, OperationHandle] =
    ZIO.succeed(new OperationHandle(OperationType.GetTables))

  override def getSchemas(
    sessionHandle: SessionHandle,
    catalogName: String,
    schemaName: String
  ): ZIO[Any, Throwable, OperationHandle] = ZIO.succeed(new OperationHandle(OperationType.GetSchemas))
}

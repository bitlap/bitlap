/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit.server

import org.bitlap.network.OperationType
import org.bitlap.network.handles.{ HandleIdentifier, OperationHandle, SessionHandle }
import org.bitlap.network.models._
import org.bitlap.network.rpc.RpcN
import org.bitlap.testkit.Dimension
import org.bitlap.tools.apply
import zio.ZIO
import scala.reflect.classTag
import com.google.protobuf.ByteString
import org.bitlap.testkit.Metric
import org.bitlap.testkit.csv.{ CsvParserBuilder, CsvParserSetting }

/**
 * Mock backend for rpc implementation.
 *
 * @author 梦境迷离
 * @version 1.0,2022/4/27
 */
@apply
class MockZioRpcBackend extends RpcN[ZIO] {

  private val input = CsvParserSetting
    .builder[Dimension]()
    .fileName("simple_data.csv")
    .classTag(classTag = classTag[Dimension])
    .dimensionName("dimensions")
    .build()

  val metrics: Seq[Metric] = CsvParserBuilder.MetricParser.fromResourceFile[Dimension](input)

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
    ZIO.succeed(new OperationHandle(OperationType.EXECUTE_STATEMENT, true, sessionHandle.handleId))

  override def fetchResults(opHandle: OperationHandle): ZIO[Any, Throwable, FetchResults] = {
    val convert = (metric: Metric) =>
      List(
        ByteString.copyFromUtf8(metric.time.toString),
        ByteString.copyFromUtf8(metric.entity.toString),
        ByteString.copyFromUtf8(metric.dimensions.map(_.map(_.value)).getOrElse(Nil).headOption.getOrElse("")),
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
          ColumnDesc("time", TypeId.TYPE_ID_INT_TYPE),
          ColumnDesc("entity", TypeId.TYPE_ID_LONG_TYPE),
          ColumnDesc("dimensions", TypeId.TYPE_ID_STRING_TYPE), // TODO not support object type
          ColumnDesc("metric_name", TypeId.TYPE_ID_STRING_TYPE),
          ColumnDesc("metric_value", TypeId.TYPE_ID_INT_TYPE)
        )
      )
    )

  override def getColumns(
    sessionHandle: SessionHandle,
    schemaName: String,
    tableName: String,
    columnName: String
  ): ZIO[Any, Throwable, OperationHandle] = ZIO.effect(new OperationHandle(OperationType.GET_COLUMNS))

  override def getDatabases(pattern: String): ZIO[Any, Throwable, OperationHandle] =
    ZIO.succeed(new OperationHandle(OperationType.GET_SCHEMAS)) // TODO add rpc method ??

  override def getTables(database: String, pattern: String): ZIO[Any, Throwable, OperationHandle] =
    ZIO.succeed(new OperationHandle(OperationType.GET_TABLES))

  override def getSchemas(
    sessionHandle: SessionHandle,
    catalogName: String,
    schemaName: String
  ): ZIO[Any, Throwable, OperationHandle] = ZIO.succeed(new OperationHandle(OperationType.GET_SCHEMAS))
}

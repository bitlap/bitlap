/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit.server

import org.bitlap.network.OperationType
import org.bitlap.network.handles.{ HandleIdentifier, OperationHandle, SessionHandle }
import org.bitlap.network.models._
import org.bitlap.network.rpc.RpcN
import org.bitlap.tools.apply
import zio.ZIO

/**
 * Mock backend for rpc implementation.
 *
 * @author 梦境迷离
 * @version 1.0,2022/4/27
 */
@apply
class MockZioRpcBackend extends RpcN[ZIO] {

  // 底层都基于ZIO，错误使用 IO.failed(new Exception)
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

  override def fetchResults(opHandle: OperationHandle): ZIO[Any, Throwable, FetchResults] =
    ZIO.succeed(FetchResults(false, RowSet()))

  override def getResultSetMetadata(opHandle: OperationHandle): ZIO[Any, Throwable, TableSchema] =
    ZIO.succeed(
      TableSchema.apply(
        List(
          ColumnDesc("time", TypeId.TYPE_ID_INT_TYPE),
          ColumnDesc("entity", TypeId.TYPE_ID_LONG_TYPE),
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

  override def getDatabases(pattern: String): ZIO[Any, Throwable, OperationHandle] = ???

  override def getTables(database: String, pattern: String): ZIO[Any, Throwable, OperationHandle] = ???

  override def getSchemas(
    sessionHandle: SessionHandle,
    catalogName: String,
    schemaName: String
  ): ZIO[Any, Throwable, OperationHandle] = ???
}

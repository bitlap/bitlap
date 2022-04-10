/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import org.bitlap.network.handles.{ OperationHandle, SessionHandle }
import org.bitlap.network.models.{ RowSet, TableSchema }
import org.bitlap.network.session.SessionManager
import org.bitlap.network.operation.OperationType

/**
 * @author 梦境迷离
 * @since 2021/11/20
 * @version 1.0
 */
class NetworkServiceImpl(private val sessionManager: SessionManager) extends NetworkService {

  override def openSession(
    username: String,
    password: String,
    configuration: Map[String, String] = Map.empty
  ): SessionHandle = {
    val session = sessionManager.openSession(username, password, configuration)
    session.sessionHandle
  }

  override def closeSession(sessionHandle: SessionHandle) {
    sessionManager.closeSession(sessionHandle)
  }

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    confOverlay: Map[String, String]
  ): OperationHandle = {
    val session = sessionManager.getSession(sessionHandle)
    sessionManager.refreshSession(sessionHandle, session)
    session.executeStatement(sessionHandle, statement, confOverlay)
  }

  override def executeStatement(
    sessionHandle: SessionHandle,
    statement: String,
    queryTimeout: Long,
    confOverlay: Map[String, String] = Map.empty
  ): OperationHandle = {
    val session = sessionManager.getSession(sessionHandle)
    sessionManager.refreshSession(sessionHandle, session)
    session.executeStatement(
      sessionHandle,
      statement,
      confOverlay,
      queryTimeout
    )
  }

  override def fetchResults(opHandle: OperationHandle): RowSet = {
    val operation = sessionManager.operationManager.getOperation(opHandle)
    val session = operation.parentSession
    sessionManager.refreshSession(session.sessionHandle, session)

    session.fetchResults(opHandle)
    //        return RowSet(
    //            listOf(
    //                Row(
    //                    listOf(
    //                        ByteString.copyFromUtf8(1112.toString()),
    //                        ByteString.copyFromUtf8("张三"),
    //                        ByteString.copyFromUtf8(12222.3232.toString()),
    //                        ByteString.copyFromUtf8(1.toString()),
    //                        ByteString.copyFromUtf8(99912211111113232L.toString()),
    //                        ByteString.copyFromUtf8(false.toString()),
    //                        ByteString.copyFromUtf8(1630332338000L.toString()),
    //
    //                        )
    //                ),
    //                Row(
    //                    listOf(
    //                        ByteString.copyFromUtf8(12222.toString()),
    //                        ByteString.copyFromUtf8("李四"),
    //                        ByteString.copyFromUtf8(10089.3232.toString()),
    //                        ByteString.copyFromUtf8(11.toString()),
    //                        ByteString.copyFromUtf8(99912211111113232L.toString()),
    //                        ByteString.copyFromUtf8(false.toString()),
    //                        ByteString.copyFromUtf8(1630332338000L.toString()),
    //                    )
    //                )
    //            )
    //        )
  }

  override def getResultSetMetadata(opHandle: OperationHandle): TableSchema = {
    val operation = sessionManager.operationManager.getOperation(opHandle)
    val session = operation.parentSession
    sessionManager.refreshSession(session.sessionHandle, session)
    session.getResultSetMetadata(opHandle)
    //        return TableSchema(
    //            listOf(
    //                ColumnDesc("ID", TypeId.B_TYPE_ID_INT_TYPE),
    //                ColumnDesc("NAME", TypeId.B_TYPE_ID_STRING_TYPE),
    //                ColumnDesc("SALARY", TypeId.B_TYPE_ID_DOUBLE_TYPE),
    //                ColumnDesc("SHORT_COL", TypeId.B_TYPE_ID_SHORT_TYPE),
    //                ColumnDesc("LONG_COL", TypeId.B_TYPE_ID_LONG_TYPE),
    //                ColumnDesc("BOOLEAN_COL", TypeId.B_TYPE_ID_BOOLEAN_TYPE),
    //                ColumnDesc("TIMESTAMP_COL", TypeId.B_TYPE_ID_TIMESTAMP_TYPE)
    //
    //            )
    //        )
  }

  override def getColumns(
    sessionHandle: SessionHandle,
    tableName: String = null,
    schemaName: String = null,
    columnName: String = null
  ): OperationHandle =
    new OperationHandle(OperationType.GET_COLUMNS)

  override def getTables(
    sessionHandle: SessionHandle,
    tableName: String = null,
    schemaName: String = null
  ): OperationHandle =
    new OperationHandle(OperationType.GET_TABLES)

  override def getSchemas(sessionHandle: SessionHandle): OperationHandle =
    new OperationHandle(OperationType.GET_SCHEMAS)
}

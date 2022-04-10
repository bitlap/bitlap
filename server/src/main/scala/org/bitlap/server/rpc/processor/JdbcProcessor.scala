/* Copyright (c) 2022 bitlap.org */
package org.bitlap.server.rpc.processor

import org.bitlap.network.helper.JdbcHelper
import org.bitlap.network.types.handles.{OperationHandle, SessionHandle}
import org.bitlap.network.types.models.{RowSet, TableSchema}
import org.bitlap.server.rpc.SessionManager

/**
 * @author 梦境迷离
 * @since 2021/11/20
 * @version 1.0
 */
class JdbcProcessor extends JdbcHelper {

  private val sessionManager = new SessionManager()
  sessionManager.startListener()

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
  ): OperationHandle = {
    import org.bitlap.network.types.OperationType
    new OperationHandle(OperationType.GET_COLUMNS)
  }

  /**
   * get databases or schemas from catalog
   *
   * @see `show databases`
   */
  override def getDatabases(pattern: String): List[String] = ???

  /**
   * get tables from catalog with database name
   *
   * @see `show tables in [db_name]`
   */
  override def getTables(database: String, pattern: String): List[String] = ???
}

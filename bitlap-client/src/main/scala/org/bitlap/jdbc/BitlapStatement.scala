/* Copyright (c) 2022 bitlap.org */
package org.bitlap.jdbc

import org.bitlap.client.BitlapClient
import org.bitlap.network.handles._
import java.sql.SQLException
import org.bitlap.network.OperationState._

import java.sql._

/** bitlap Statement
 *
 *  @author
 *    梦境迷离
 *  @since 2021/6/6
 *  @version 1.0
 */
class BitlapStatement(
  private val sessHandle: SessionHandle,
  private val client: BitlapClient
) extends Statement {

  private var stmtHandle: OperationHandle = _
  private var fetchSize                   = 50
  private var queryTimeout                = 30

  /** We need to keep a reference to the result set to support the following:
   *
   *  statement.execute(String sql); statement.getResultSet();
   */
  private var resultSet: ResultSet = _

  /** The maximum number of rows this statement should return (0 => all rows)
   */
  private var maxRows = 0

  /** Add SQLWarnings to the warningChain if needed
   */
  private var warningChain: SQLWarning = _

  /** Keep state so we can fail certain calls made after close();
   */
  private var closed = false

  /** Keep state so we can fail certain calls made after cancel().
   */
  private var isCancelled = false

  override def unwrap[T](iface: Class[T]): T = ???

  override def isWrapperFor(iface: Class[_]): Boolean = ???

  override def close() {
    // TODO: how to properly shut down the client?
    resultSet = null
    closed = true
  }

  override def executeQuery(sql: String): ResultSet = {
    if (!execute(sql)) {
      throw BitlapSQLException("The query did not generate a result set!")
    }
    resultSet
  }

  override def executeUpdate(sql: String): Int = ???

  override def getMaxFieldSize: Int = ???

  override def setMaxFieldSize(max: Int): Unit = ???

  override def getMaxRows: Int = maxRows

  override def setMaxRows(max: Int): Unit = {
    if (max < 0) throw new SQLException("max must be >= 0")
    maxRows = max
  }

  override def setEscapeProcessing(enable: Boolean): Unit = ???

  override def getQueryTimeout: Int = queryTimeout

  override def setQueryTimeout(seconds: Int): Unit =
    queryTimeout = seconds

  override def cancel(): Unit = {
    if (isCancelled) return

    try
      if (stmtHandle != null) {
        client.cancelOperation(stmtHandle)
      }
    catch {
      case e: SQLException =>
        throw e
      case e: Exception =>
        throw new SQLException("Failed to cancel statement", "08S01", e)
    }

    isCancelled = true
  }

  override def getWarnings: SQLWarning = warningChain

  override def executeUpdate(sql: String, autoGeneratedKeys: Int): Int = ???

  override def executeUpdate(sql: String, columnIndexes: scala.Array[Int]): Int = ???

  override def executeUpdate(sql: String, columnNames: scala.Array[String]): Int = ???

  override def clearWarnings(): Unit = warningChain = null

  override def setCursorName(name: String): Unit = ???

  override def execute(sql: String): Boolean = {
    if (closed) throw BitlapSQLException("Can't execute after statement has been closed")
    try {
      resultSet = null
      stmtHandle = client.executeStatement(sessHandle, sql, queryTimeout)
      if (stmtHandle == null || !stmtHandle.hasResultSet) {
        return false
      }
    } catch {
      case ex: Exception => throw BitlapSQLException(ex.toString, cause = ex)
    }

    var operationComplete = false

    while (!operationComplete)
      try {
        val statusResp = client.getOperationStatus(stmtHandle)
        statusResp match {
          case ClosedState | FinishedState => operationComplete = true
          case CanceledState =>
            throw new SQLException("Query was cancelled", "01000")
          case UnknownState =>
          case ErrorState =>
            throw new SQLException("Query was failed", "HY000")
          case _ =>
        }
      } catch {
        case e: SQLException => throw e
        case e: Exception =>
          throw new SQLException(e.toString, "08S01", e)
      }

    resultSet = BitlapQueryResultSet
      .builder()
      .setClient(client)
      .setSessionHandle(sessHandle)
      .setStmtHandle(stmtHandle)
      .setMaxRows(maxRows)
      .setFetchSize(fetchSize)
      .build()
    true
  }

  override def execute(sql: String, autoGeneratedKeys: Int): Boolean = ???

  override def execute(sql: String, columnIndexes: scala.Array[Int]): Boolean = ???

  override def execute(sql: String, columnNames: scala.Array[String]): Boolean = ???

  override def getResultSet(): ResultSet = resultSet

  override def getUpdateCount(): Int = ???

  override def getMoreResults(): Boolean = false

  override def getMoreResults(current: Int): Boolean = ???

  override def setFetchDirection(direction: Int): Unit = ???

  override def getFetchDirection(): Int = ResultSet.FETCH_FORWARD

  override def setFetchSize(rows: Int): Unit =
    if (rows > 0) fetchSize = rows
    else if (rows == 0) fetchSize = 50
    else throw new SQLException("Fetch size must be greater or equal to 0")

  override def getFetchSize(): Int = fetchSize

  override def getResultSetConcurrency(): Int = ???

  override def getResultSetType(): Int = ResultSet.TYPE_FORWARD_ONLY

  override def addBatch(sql: String): Unit = ???

  override def clearBatch(): Unit = ???

  override def executeBatch(): scala.Array[Int] = ???

  override def getConnection(): Connection = ???

  override def getGeneratedKeys(): ResultSet = ???

  override def getResultSetHoldability(): Int = ???

  override def isClosed(): Boolean = closed

  override def setPoolable(poolable: Boolean): Unit = ???

  override def isPoolable(): Boolean = ???

  override def closeOnCompletion(): Unit = ???

  override def isCloseOnCompletion(): Boolean = ???
}

/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.jdbc

import java.sql.*

import org.bitlap.client.BitlapClient
import org.bitlap.network.enumeration.OperationState.*
import org.bitlap.network.handles.*
import org.bitlap.network.models.*

/** Bitlap statement
 */
class BitlapStatement(
  private val connection: Connection,
  private val sessHandle: SessionHandle,
  private var client: BitlapClient)
    extends Statement:

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

  /** Keep this state so we can know whether the query in this statement is closed.
   */
  private var isQueryClosed = false

  private var isOperationComplete = false

  private var _closeOnResultSetCompletion = false

  private def checkConnection(action: String): Unit =
    if closed then throw BitlapSQLException(s"Cannot $action after statement has been closed")

  override def unwrap[T](iface: Class[T]): T = throw new SQLFeatureNotSupportedException("Method not supported")

  override def isWrapperFor(iface: Class[?]): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  private def closeClientOperation(): Unit =
    try closeStatementIfNeeded()
    finally isQueryClosed = true

  /** Closes the statement if there is one running. Do not change the the flags.
   *
   *  @throws SQLException
   *    If there is an error closing the statement
   */
  private def closeStatementIfNeeded(): Unit =
    try if stmtHandle != null then client.closeOperation(stmtHandle)
    catch
      case e: SQLException =>
        throw BitlapSQLException(msg = e.getLocalizedMessage, cause = Option(e.getCause))
      case e: Exception =>
        throw BitlapSQLException(msg = "Failed to close statement", "08S01", cause = Option(e))
    finally stmtHandle = null

  override def close(): Unit =
    if closed then return
    closeClientOperation()
    client = null
    if resultSet != null then
      if !resultSet.isClosed then resultSet.close()
      resultSet = null
    closed = true

  override def executeQuery(sql: String): ResultSet =
    if !execute(sql) then throw BitlapSQLException("The query did not generate a result set!")
    resultSet

  override def executeUpdate(sql: String): Int =
    execute(sql)
    0

  override def getMaxFieldSize: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def setMaxFieldSize(max: Int): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getMaxRows: Int =
    checkConnection("getMaxRows")
    maxRows

  override def setMaxRows(max: Int): Unit =
    checkConnection("setMaxRows")
    if max < 0 then throw new BitlapSQLException("max must be >= 0")
    maxRows = max

  override def setEscapeProcessing(enable: Boolean): Unit = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getQueryTimeout: Int =
    checkConnection("getQueryTimeout")
    queryTimeout

  override def setQueryTimeout(seconds: Int): Unit =
    queryTimeout = seconds

  override def cancel(): Unit =
    checkConnection("cancel")
    if isCancelled then return

    try if stmtHandle != null then client.cancelOperation(stmtHandle)
    catch
      case e: SQLException =>
        throw e
      case e: Exception =>
        throw BitlapSQLException("Failed to cancel statement", cause = Option(e))

    isCancelled = true

  override def getWarnings: SQLWarning =
    checkConnection("getWarnings")
    warningChain

  override def executeUpdate(sql: String, autoGeneratedKeys: Int): Int = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def executeUpdate(sql: String, columnIndexes: scala.Array[Int]): Int =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def executeUpdate(sql: String, columnNames: scala.Array[String]): Int =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def clearWarnings(): Unit = warningChain = null

  override def setCursorName(name: String): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  private def reInitState(): Unit =
    try closeStatementIfNeeded()
    finally
      isCancelled = false
      isQueryClosed = false
      isOperationComplete = false

  override def execute(sql: String): Boolean =
    checkConnection("execute")
    reInitState()
    try
      resultSet = null
      stmtHandle = client.executeStatement(sessHandle, sql, queryTimeout)
      if stmtHandle == null || !stmtHandle.hasResultSet then return false
    catch
      case ex: Throwable =>
        throw BitlapSQLException(s"${ex.getLocalizedMessage}")

    val status = waitForOperationToComplete()
    // The query should be completed by now
    if !status.hasResultSet.getOrElse(false) || stmtHandle != null && !stmtHandle.hasResultSet then return false

    resultSet = BitlapQueryResultSet
      .builder(this)
      .setClient(client)
      .setStmtHandle(stmtHandle)
      .setMaxRows(maxRows)
      .setFetchSize(fetchSize)
      .build()
    true

  def waitForOperationToComplete(): OperationStatus =
    var status: OperationStatus = null
    while !isOperationComplete do
      try
        if Thread.currentThread.isInterrupted then throw BitlapSQLException("Interrupted wait for operation!")
        status = client.getOperationStatus(stmtHandle)
        status.status match
          case Some(ClosedState | FinishedState) => isOperationComplete = true
          case Some(CanceledState) =>
            throw BitlapSQLException("Query was cancelled", "01000")
          case Some(TimeoutState) =>
            throw new SQLTimeoutException(s"Query timed out after $queryTimeout seconds")
          case Some(ErrorState) =>
            throw BitlapSQLException("Query was failed", "HY000")
          case Some(UnknownState) =>
            throw BitlapSQLException("Unknown query", "HY000")
          case _ =>
      catch
        case e: SQLException => throw e
        case e: Exception =>
          throw BitlapSQLException("Failed to wait for operation to complete", "08S01", cause = Option(e))
    status

  def closeOnResultSetCompletion: Unit =
    if _closeOnResultSetCompletion then
      resultSet = null
      close()

  override def execute(sql: String, autoGeneratedKeys: Int): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def execute(sql: String, columnIndexes: scala.Array[Int]): Boolean =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def execute(sql: String, columnNames: scala.Array[String]): Boolean =
    throw new SQLFeatureNotSupportedException("Method not supported")

  override def getResultSet(): ResultSet =
    checkConnection("getResultSet")
    resultSet

  override def getUpdateCount(): Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getMoreResults(): Boolean = false

  override def getMoreResults(current: Int): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def setFetchDirection(direction: Int): Unit =
    checkConnection("setFetchDirection")
    if direction != ResultSet.FETCH_FORWARD then throw BitlapSQLException(s"Not supported direction: $direction")

  override def getFetchDirection(): Int =
    checkConnection("getFetchDirection")
    ResultSet.FETCH_FORWARD

  override def setFetchSize(rows: Int): Unit =
    checkConnection("setFetchSize")
    if rows > 0 then fetchSize = rows
    else if rows == 0 then fetchSize = 50
    else throw BitlapSQLException("Fetch size must be greater or equal to 0")

  override def getFetchSize(): Int =
    checkConnection("getFetchSize")
    fetchSize

  override def getResultSetConcurrency(): Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getResultSetType(): Int =
    checkConnection("getResultSetType")
    ResultSet.TYPE_FORWARD_ONLY

  override def addBatch(sql: String): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def clearBatch(): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def executeBatch(): scala.Array[Int] = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getConnection(): Connection =
    checkConnection("getConnection")
    connection

  override def getGeneratedKeys(): ResultSet = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getResultSetHoldability(): Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def isClosed(): Boolean = closed

  override def setPoolable(poolable: Boolean): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def isPoolable(): Boolean = throw new SQLFeatureNotSupportedException("Method not supported")

  override def closeOnCompletion(): Unit =
    _closeOnResultSetCompletion = true

  override def isCloseOnCompletion(): Boolean = _closeOnResultSetCompletion

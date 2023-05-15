/* Copyright (c) 2023 bitlap.org */
package org.bitlap.jdbc

import java.sql.*

import scala.collection.mutable

import org.bitlap.client.BitlapClient
import org.bitlap.jdbc.BitlapQueryResultSet.Builder
import org.bitlap.network.handles.*
import org.bitlap.network.models.*
import org.bitlap.network.models.OperationStatus

/** bitlap 查询的结果集
 *  @author
 *    梦境迷离
 *  @since 2021/6/12
 *  @version 1.0
 */
class BitlapQueryResultSet(private var client: BitlapClient, private var maxRows: Int, private val _row: Row = null)
    extends BitlapBaseResultSet:

  private var warningChain: SQLWarning                = _
  private var row: Row                                = _row
  private val columnNames: mutable.ListBuffer[String] = mutable.ListBuffer.empty
  private val columnTypes: mutable.ListBuffer[String] = mutable.ListBuffer.empty

  private var emptyResultSet                   = false
  private var rowsFetched                      = 0
  protected var closed: Boolean                = false
  protected var fetchSize                      = 0
  private var fetchFirst                       = false
  private var fetchedRows: List[Row]           = List.empty
  private var fetchedRowsItr: Iterator[Row]    = fetchedRows.iterator
  private var stmtHandle: OperationHandle      = _
  private var operationStatus: OperationStatus = _
  private var statement: Statement             = _

  override protected def curRow: Row = row

  def this(builder: Builder) =
    this(builder.client, builder.maxRows)
    this.client = builder.client
    this.statement = builder.statement
    this.stmtHandle = builder.stmtHandle
    this.fetchSize = builder.fetchSize
    if builder.retrieveSchema then retrieveSchema()
    else
      this.columnNames.appendAll(builder.colNames)
      this.columnTypes.appendAll(builder.colTypes)
    this.emptyResultSet = builder.emptyResultSet
    maxRows =
      if builder.emptyResultSet then 0
      else builder.maxRows

  private def checkResultSet(action: String): Unit =
    if closed || client == null || stmtHandle == null then
      throw BitlapSQLException(s"Cannot $action after Resultset has been closed")

  private def checkClose(action: String): Unit =
    if closed then throw BitlapSQLException(s"Cannot $action after Resultset has been closed")

  private def retrieveSchema(): Unit =
    try
      checkResultSet("<init>")
      // debug
      val namesSb = new mutable.StringBuilder()
      val typesSb = new mutable.StringBuilder()

      val schema = client.getResultSetMetadata(stmtHandle)
      if schema == null || schema.columns.isEmpty then return

      this.setSchema(schema)
      val columns = schema.columns
      for pos <- schema.columns.indices do
        if pos != 0 then
          namesSb.append(",")
          typesSb.append(",")
        val columnName = columns(pos).columnName
        columnNames.append(columnName)
        val columnTypeName = columns(pos).typeDesc.name
        columnTypes.append(columnTypeName)
        namesSb.append(columnName)
        typesSb.append(columnTypeName)
    catch
      case e: Exception => throw BitlapSQLException(s"Could not create ResultSet: ${e.getMessage}", cause = Option(e))

  override def next(): Boolean =
    checkResultSet("next")
    if emptyResultSet || (1 to rowsFetched).contains(maxRows) then return false

    statement match
      case st: BitlapStatement if operationStatus == null || !operationStatus.hasResultSet.getOrElse(false) =>
        operationStatus = st.waitForOperationToComplete()
      case _ =>

    try

      if fetchFirst then
        fetchedRows = null
        fetchedRowsItr = null
        fetchFirst = false

      if fetchedRows.isEmpty || !fetchedRowsItr.hasNext then
        val result = client.fetchResults(stmtHandle, maxRows = fetchSize, 1)
        if result != null then
          fetchedRows = result.rows
          fetchedRowsItr = fetchedRows.iterator
      if fetchedRowsItr.hasNext then row = fetchedRowsItr.next()
      else return false

      rowsFetched = rowsFetched + 1
    catch case e: Exception => throw BitlapSQLException(msg = "Error retrieving next row", cause = Option(e))

    true

  override def isClosed(): Boolean =
    this.closed

  override def getMetaData(): ResultSetMetaData =
    checkClose("getMetaData")
    super.getMetaData()

  override def getFetchSize(): Int =
    checkClose("getFetchSize")
    this.fetchSize

  override def setFetchSize(rows: Int): Unit =
    checkClose("setFetchSize")
    this.fetchSize = rows

  private def closeOperationHandle(stmtHandle: OperationHandle): Unit =
    try if stmtHandle != null then client.closeOperation(stmtHandle)
    catch
      case e: Exception =>
        throw BitlapSQLException(e.toString, "08S01", cause = Option(e))

  override def close(): Unit =
    if this.statement != null && this.statement.isInstanceOf[BitlapStatement] then
      val s = this.statement.asInstanceOf[BitlapStatement]
      s.closeOnResultSetCompletion
    else closeOperationHandle(stmtHandle)

    client = null
    stmtHandle = null
    closed = true
    operationStatus = null

  override def beforeFirst(): Unit =
    checkClose("beforeFirst")
    fetchFirst = true
    rowsFetched = 0

  override def getType: Int =
    checkClose("getType")
    ResultSet.TYPE_FORWARD_ONLY

  override def isBeforeFirst: Boolean =
    checkClose("isBeforeFirst")
    rowsFetched == 0

  override def getRow: Int = rowsFetched

  override def getWarnings(): SQLWarning = warningChain

  override def clearWarnings(): Unit = warningChain = null

object BitlapQueryResultSet:

  def builder(): Builder = new Builder(null.asInstanceOf[Statement])

  def builder(statement: Statement): Builder = new Builder(statement)

  final class Builder(_statement: Statement):
    val statement: Statement        = _statement
    var client: BitlapClient        = _
    var stmtHandle: OperationHandle = _

    /** Sets the limit for the maximum number of rows that any ResultSet object produced by this Statement can contain
     *  to the given number. If the limit is exceeded, the excess rows are silently dropped. The value must be >= 0, and
     *  0 means there is not limit.
     */
    var maxRows                = 0
    var retrieveSchema         = true
    var colNames: List[String] = List.empty
    var colTypes: List[String] = List.empty
    var fetchSize              = 50
    var emptyResultSet         = false

    def setClient(client: BitlapClient): Builder =
      this.client = client
      this

    def setStmtHandle(stmtHandle: OperationHandle): Builder =
      this.stmtHandle = stmtHandle
      this

    def setMaxRows(maxRows: Int): Builder =
      this.maxRows = maxRows
      this

    def setSchema(colNames: List[String], colTypes: List[String]): Builder =
      this.colNames ++= colNames
      this.colTypes ++= colTypes
      retrieveSchema = false
      this

    def setFetchSize(fetchSize: Int): Builder =
      this.fetchSize = fetchSize
      this

    def setEmptyResultSet(emptyResultSet: Boolean): Builder =
      this.emptyResultSet = emptyResultSet
      this

    def build(): BitlapQueryResultSet =
      new BitlapQueryResultSet(this)

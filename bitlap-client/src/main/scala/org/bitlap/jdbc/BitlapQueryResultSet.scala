/* Copyright (c) 2022 bitlap.org */
package org.bitlap.jdbc

import org.bitlap.client.BitlapClient
import org.bitlap.jdbc.BitlapQueryResultSet.Builder
import org.bitlap.network.handles._
import org.bitlap.network.models._
import scala.collection.mutable
import java.sql._
import org.bitlap.network.models.OperationStatus

/** bitlap 查询的结果集
 *  @author
 *    梦境迷离
 *  @since 2021/6/12
 *  @version 1.0
 */
class BitlapQueryResultSet(
  private var client: BitlapClient,
  private var maxRows: Int,
  override var row: Row = null
) extends BitlapBaseResultSet() {

  override protected var warningChain: SQLWarning  = _
  override protected var columnNames: List[String] = List.empty
  override protected var columnTypes: List[String] = List.empty

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

  def this(builder: Builder) = {
    this(builder.client, builder.maxRows)
    this.client = builder.client
    this.statement = builder.statement
    this.stmtHandle = builder.stmtHandle
    this.fetchSize = builder.fetchSize
    if (builder.retrieveSchema) {
      retrieveSchema()
    } else {
      this.columnNames ++= builder.colNames
      this.columnTypes ++= builder.colTypes
    }
    this.emptyResultSet = builder.emptyResultSet
    maxRows = if (builder.emptyResultSet) {
      0
    } else {
      builder.maxRows
    }
  }

  private def retrieveSchema(): Unit =
    try {
      if (client == null || stmtHandle == null) {
        throw BitlapSQLException("Resultset is closed")
      }
      // debug
      val namesSb = new mutable.StringBuilder()
      val typesSb = new mutable.StringBuilder()

      val schema = client.getResultSetMetadata(stmtHandle)
      if (schema == null || schema.columns.isEmpty) {
        return
      }

      this.setSchema(schema)
      val columns = schema.columns
      for (pos <- schema.columns.indices) {
        if (pos != 0) {
          namesSb.append(",")
          typesSb.append(",")
        }
        val columnName = columns(pos).columnName
        columnNames :+= columnName
        val columnTypeName = columns(pos).typeDesc.name
        columnTypes :+= columnTypeName
        namesSb.append(columnName)
        typesSb.append(columnTypeName)
      }
    } catch {
      case e: Exception => throw BitlapSQLException(s"Could not create ResultSet: ${e.getMessage}", cause = e)
    }

  override def next(): Boolean = {
    if (closed || client == null || stmtHandle == null) {
      throw BitlapSQLException("Resultset is closed")
    }
    if (emptyResultSet || (1 to rowsFetched).contains(maxRows)) {
      return false
    }

    statement match {
      case st: BitlapStatement if operationStatus == null || !operationStatus.hasResultSet.getOrElse(false) =>
        operationStatus = st.waitForOperationToComplete()
      case _ =>
    }

    try {

      if (fetchFirst) {
        fetchedRows = null
        fetchedRowsItr = null
        fetchFirst = false
      }

      if (fetchedRows.isEmpty || !fetchedRowsItr.hasNext) {
        val result = client.fetchResults(stmtHandle, maxRows = fetchSize, 1)
        if (result != null) {
          fetchedRows = result.rows
          fetchedRowsItr = fetchedRows.iterator
        }
      }
      if (fetchedRowsItr.hasNext) {
        row = fetchedRowsItr.next()
      } else {
        return false
      }

      rowsFetched = rowsFetched + 1
    } catch {
      case e: Exception => throw BitlapSQLException(msg = "Error retrieving next row", cause = e)
    }

    true
  }

  override def isClosed(): Boolean =
    this.closed

  override def getMetaData(): ResultSetMetaData = {
    if (closed) {
      throw BitlapSQLException("Resultset is closed")
    }
    super.getMetaData()
  }

  override def getFetchSize(): Int = {
    if (closed) {
      throw BitlapSQLException("Resultset is closed")
    }
    this.fetchSize
  }

  override def setFetchSize(rows: Int): Unit = {
    if (closed) {
      throw BitlapSQLException("Resultset is closed")
    }
    this.fetchSize = rows
  }

  private def closeOperationHandle(stmtHandle: OperationHandle): Unit =
    try
      if (stmtHandle != null) {
        client.closeOperation(stmtHandle)
      }
    catch {
      case e: Exception =>
        throw BitlapSQLException(e.toString, "08S01", cause = e)
    }
  override def close(): Unit = {
    if (this.statement != null && this.statement.isInstanceOf[BitlapStatement]) {
      val s = this.statement.asInstanceOf[BitlapStatement]
      s.closeOnResultSetCompletion
    } else {
      closeOperationHandle(stmtHandle)
    }

    client = null
    stmtHandle = null
    closed = true
    operationStatus = null
  }

  override def beforeFirst(): Unit = {
    if (closed) throw new SQLException("Resultset is closed")
    fetchFirst = true
    rowsFetched = 0
  }

  override def getType: Int =
    if (closed) throw new SQLException("Resultset is closed")
    else ResultSet.TYPE_FORWARD_ONLY

  override def isBeforeFirst: Boolean = {
    if (closed) throw new SQLException("Resultset is closed")
    rowsFetched == 0
  }

  override def getRow: Int = rowsFetched
}

object BitlapQueryResultSet {

  def builder(): Builder = new Builder(null.asInstanceOf[Statement])

  def builder(statement: Statement): Builder = new Builder(statement)

  class Builder(_statement: Statement) {
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

    def setClient(client: BitlapClient): Builder = {
      this.client = client
      this
    }

    def setStmtHandle(stmtHandle: OperationHandle): Builder = {
      this.stmtHandle = stmtHandle
      this
    }

    def setMaxRows(maxRows: Int): Builder = {
      this.maxRows = maxRows
      this
    }

    def setSchema(colNames: List[String], colTypes: List[String]): Builder = {
      this.colNames ++= colNames
      this.colTypes ++= colTypes
      retrieveSchema = false
      this
    }

    def setFetchSize(fetchSize: Int): Builder = {
      this.fetchSize = fetchSize
      this
    }

    def setEmptyResultSet(emptyResultSet: Boolean): Builder = {
      this.emptyResultSet = emptyResultSet
      this
    }

    def build(): BitlapQueryResultSet =
      new BitlapQueryResultSet(this)
  }
}

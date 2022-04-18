/* Copyright (c) 2022 bitlap.org */
package org.bitlap.jdbc

import org.bitlap.jdbc.BitlapQueryResultSet.Builder
import org.bitlap.network.proto.driver.{ BOperationHandle, BRow, BSessionHandle }

import java.sql.{ ResultSetMetaData, SQLException, SQLWarning }
import scala.jdk.CollectionConverters._

/**
 * @author 梦境迷离
 * @since 2021/6/12
 * @version 1.0
 */
class BitlapQueryResultSet(
  private var client: BitlapClient,
  private var maxRows: Int,
  override var row: BRow = null
) extends BitlapBaseResultSet() {
  override protected var warningChain: SQLWarning = _
  override protected var columnNames: List[String] = List.empty
  override protected var columnTypes: List[String] = List.empty

  private var emptyResultSet = false
  private var rowsFetched = 0
  protected var closed: Boolean = false
  protected var fetchSize = 0

  private var fetchedRows: List[BRow] = List.empty
  private var fetchedRowsItr: Iterator[BRow] = fetchedRows.iterator
  private var sessHandle: BSessionHandle = _
  private var stmtHandle: BOperationHandle = _

  def this(builder: Builder) = {
    this(builder.client, builder.maxRows)
    this.client = builder.client
    this.stmtHandle = builder.stmtHandle
    this.sessHandle = builder.sessHandle
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
        throw BSQLException("Resultset is closed")
      }
      val namesSb = new StringBuilder()
      val typesSb = new StringBuilder()

      val schema = client.getResultSetMetadata(stmtHandle)
      if (schema == null || schema.getColumnsList.isEmpty) {
        return
      }

      this.setSchema(schema)
      val columns = schema.getColumnsList
      for (pos <- 0 until schema.getColumnsCount) {
        if (pos != 0) {
          namesSb.append(",")
          typesSb.append(",")
        }
        val columnName = columns.get(pos).getColumnName
        columnNames :+= columnName
        val columnTypeName = Utils.SERVER_TYPE_NAMES(columns.get(pos).getTypeDesc).stringify
        columnTypes :+= columnTypeName
        namesSb.append(columnName)
        typesSb.append(columnTypeName)
      }
    } catch {
      case e: SQLException => throw BSQLException(s"Could not create ResultSet: ${e.getMessage}", cause = e)
      case e: Exception    => throw e
    }

  override def next(): Boolean = {
    if (closed || client == null || stmtHandle == null) {
      throw BSQLException("Resultset is closed")
    }
    if (emptyResultSet || (1 to rowsFetched).contains(maxRows)) {
      return false
    }
    try {

      if (fetchedRows.isEmpty || !fetchedRowsItr.hasNext) {
        val result = client.fetchResults(stmtHandle)
        if (result != null) {
          fetchedRows = result.getResults.getRowsList.asScala.toList
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
      case e: SQLException => throw BSQLException(msg = "Error retrieving next row", cause = e)
      case e: Exception    => throw e
    }

    true
  }

  override def isClosed(): Boolean =
    this.closed

  override def getMetaData(): ResultSetMetaData = {
    if (closed) {
      throw new SQLException("Resultset is closed")
    }
    super.getMetaData()
  }

  override def getFetchSize(): Int = {
    if (closed) {
      throw new SQLException("Resultset is closed")
    }
    this.fetchSize
  }

  override def setFetchSize(rows: Int): Unit = {
    if (closed) {
      throw BSQLException("Resultset is closed")
    }
    this.fetchSize = rows
  }

  override def close(): Unit = {
    this.client = null
    this.stmtHandle = null
    this.sessHandle = null
    this.closed = true
  }
}

object BitlapQueryResultSet {
  def builder(): Builder = new Builder()

  class Builder {
    var client: BitlapClient = _
    var stmtHandle: BOperationHandle = _
    var sessHandle: BSessionHandle = _

    /**
     * Sets the limit for the maximum number of rows that any ResultSet object produced by this
     * Statement can contain to the given number. If the limit is exceeded, the excess rows
     * are silently dropped. The value must be >= 0, and 0 means there is not limit.
     */
    var maxRows = 0
    var retrieveSchema = true
    var colNames: List[String] = List.empty
    var colTypes: List[String] = List.empty
    var fetchSize = 50
    var emptyResultSet = false

    def setClient(client: BitlapClient): Builder = {
      this.client = client
      this
    }

    def setStmtHandle(stmtHandle: BOperationHandle): Builder = {
      this.stmtHandle = stmtHandle
      this
    }

    def setSessionHandle(sessHandle: BSessionHandle): Builder = {
      this.sessHandle = sessHandle
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

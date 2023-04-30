/* Copyright (c) 2023 bitlap.org */
package org.bitlap.spark.writer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.*
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer
import org.apache.spark.sql.catalyst.encoders.*
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.bitlap.spark.jdbc.BitlapJdbcDialect
import org.bitlap.spark.util.*

import java.io.IOException
import java.sql.*

/** @author
 *    梦境迷离
 *  @version 1.0,10/15/22
 */
class BitlapDataWriter(private val schema: StructType, private val options: BitlapDataSourceWriteOptions)
    extends DataWriter[InternalRow] {

  private final val DEFAULT_BATCH_SIZE     = 100
  private var conn: Connection             = _
  private var statement: PreparedStatement = _
  private var numRecords                   = 0

  private lazy val _schema: StructType = options.schema
  private lazy val batchSize =
    String.valueOf(options.overriddenProps.getOrDefault("options", DEFAULT_BATCH_SIZE.toString)).toLong
  private lazy val attrs = schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()).toList
  private lazy val encoder: ExpressionEncoder[Row] =
    RowEncoder(schema).resolveAndBind(attrs.iterator.toSeq, SimpleAnalyzer)

  try {
    conn = DriverManager.getConnection(options.url, null)
    var colNames = options.schema.names
    if !options.skipNormalizingIdentifier then colNames = colNames.map(SchemaUtil.normalizeIdentifier)

    val upsertSql = QueryUtil.constructUpsertStatement(options.tableName, List(colNames*))
    statement = conn.prepareStatement(upsertSql)

  } catch {
    case e: SQLException =>
      throw new RuntimeException(e)
  }

  // MOCK DATA
  statement.execute("create table if not exists tb_dimension")
  statement.execute("load data 'classpath:simple_data.csv' overwrite table tb_dimension")

  override def write(internalRow: InternalRow): Unit =
    try {
      var i   = 0
      val row = SparkJdbcUtil.toRow(encoder, internalRow)
      for field <- _schema.fields do {
        val dataType = field.dataType
        if internalRow.isNullAt(i) then
          statement.setNull(i + 1, SparkJdbcUtil.getJdbcType(dataType, BitlapJdbcDialect).jdbcNullType)
        else SparkJdbcUtil.makeSetter(conn, BitlapJdbcDialect, dataType).apply(statement, row, i)
        i += 1
      }
      numRecords += 1
      statement.execute
      // Run batch wise commits only when the batch size is positive value.
      // Otherwise commit gets called at the end of task
      if batchSize > 0 && (numRecords % batchSize == 0) then {
        commitBatchUpdates()
      }
    } catch {
      case e: SQLException =>
        throw new IOException("Exception while executing Phoenix prepared statement", e)
    }

  def commitBatchUpdates(): Unit =
    conn.commit()

  override def commit(): WriterCommitMessage = {
    try conn.commit()
    catch {
      case e: SQLException =>
        throw new RuntimeException(e)
    } finally
      try {
        statement.close()
        conn.close()
      } catch {
        case ex: SQLException =>
          throw new RuntimeException(ex)
      }
    null
  }

  override def abort(): Unit =
    try
      conn.rollback()
    catch {
      case ex: SQLException =>
        throw new RuntimeException(ex)
    }

  override def close(): Unit =
    try conn.close()
    catch {
      case ex: SQLException =>
        throw new RuntimeException(ex)
    }
}

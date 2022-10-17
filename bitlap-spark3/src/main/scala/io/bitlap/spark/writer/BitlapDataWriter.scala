/* Copyright (c) 2022 bitlap.org */
package io.bitlap.spark.writer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{ DataWriter, WriterCommitMessage }
import org.apache.spark.sql.types.StructType

import java.sql.{ DriverManager, SQLException, Statement }

/** @author
 *    梦境迷离
 *  @version 1.0,10/15/22
 */
final class BitlapDataWriter(private val schema: StructType, private val options: BitlapDataSourceWriteOptions)
    extends DataWriter[InternalRow] {

  private val conn = DriverManager.getConnection(options.url)

  private val statement: Statement = conn.createStatement()

  statement.execute("create table if not exists tb_dimension")
  statement.execute("load data 'classpath:simple_data.csv' overwrite table tb_dimension")

  override def write(t: InternalRow): Unit = {}

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

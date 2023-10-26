/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.storage.io

import org.bitlap.common.exception.BitlapException
import org.bitlap.common.utils.StringEx
import org.bitlap.common.utils.StringEx.blankOr
import org.bitlap.core.hadoop._
import org.bitlap.core.sql.QueryContext
import org.bitlap.core.storage.BitlapWriter
import org.bitlap.core.storage.BitlapWriters

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopOutputFile

/** bitlap mdm model writer implemented by parquet
 */
class BitlapParquetWriter[T](
  private val fs: FileSystem,
  private val output: Path,
  private val schema: Schema,
  private val rowGroupSize: Long,
  private val rowConvert: T => GenericRecord)
    extends BitlapWriter[T] {

  private val conf = fs.newConf()
  private val txId = StringEx.blankOr(QueryContext.get().queryId, StringEx.uuid(true))

  private val outputFile =
    Path(output, BitlapWriters.genUniqueFile(txId, "mdm", s"${CompressionCodecName.SNAPPY.getExtension}.parquet"))
  private var writer: ParquetWriter[GenericRecord] = _

  override def writeBatch(rows: Iterable[T]): Unit = {
    // batch should delete output path
    if (fs.exists(output)) {
      fs.delete(output, true)
    }
    fs.mkdirs(output)

    // write rows
    initWriter()
    rows.foreach { row =>
      val r = this.rowConvert(row)
      this.writer.write(r)
    }
  }

  override def write(row: T): Unit = {
    throw BitlapException(s"BitlapMetricWriter stream write is not supported.")
  }

  private def initWriter(): Unit = {
    if (this.writer != null) {
      scala.util.Using.resource(this.writer) { _ => }
      this.writer = null
    }
    val file = HadoopOutputFile.fromPath(outputFile, conf)
    this.writer = AvroParquetWriter
      .builder[GenericRecord](file)
      .withSchema(schema)
      .withRowGroupSize(rowGroupSize)
      .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
      .withConf(conf)
      .build()
  }

  override def close(): Unit = {
    scala.util.Using.resource(this.writer) { _ => }
    this.writer = null
  }
}

/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.storage.io

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.bitlap.common.exception.BitlapException
import org.bitlap.common.utils.StringEx
import org.bitlap.common.utils.StringEx.blankOr
import org.bitlap.core.sql.QueryContext
import org.bitlap.core.storage.BitlapWriter
import org.bitlap.core.storage.BitlapWriters
import org.bitlap.core.utils.Hcfs.newConf

/**
 * Desc: bitlap mdm model writer implemented by parquet
 */
class BitlapParquetWriter<T>(
    private val fs: FileSystem,
    private val output: Path,
    private val schema: Schema,
    private val rowGroupSize: Long,
    private val rowConvert: (T) -> GenericRecord
) : BitlapWriter<T> {

    private val conf = fs.newConf()
    private val txId = QueryContext.get().queryId.blankOr(StringEx.uuid(true))
    private val outputFile = Path(output, BitlapWriters.genUniqueFile(txId, "mdm", "${CompressionCodecName.SNAPPY.extension}.parquet"))
    private var writer: ParquetWriter<GenericRecord>? = null

    override fun writeBatch(rows: Iterable<T>) {
        // batch should delete output path
        if (fs.exists(output)) {
            fs.delete(output, true)
        }
        fs.mkdirs(output)

        // write rows
        initWriter()
        rows.forEach { row ->
            this.writer!!.write(this.rowConvert.invoke(row))
        }
    }

    override fun write(row: T) {
        throw BitlapException("BitlapMetricWriter stream write is not supported.")
    }

    private fun initWriter() {
        if (this.writer != null) {
            this.writer.use { }
            this.writer = null
        }
        val file = HadoopOutputFile.fromPath(outputFile, conf)
        this.writer = AvroParquetWriter.builder<GenericRecord>(file)
            .withSchema(schema)
            .withRowGroupSize(rowGroupSize)
            .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
            .withConf(conf)
            .build()
    }

    override fun close() {
        this.writer.use { }
        this.writer = null
    }
}

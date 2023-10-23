/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.storage.io

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.bitlap.core.storage.BitlapReader

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.bitlap.core.hadoop._

/**
 * Desc: bitlap metric writer implemented by parquet
 */
class BitlapParquetReader[T](
    private val fs: FileSystem,
    private val inputs: List[Path],
    private val schema: Schema,
    private val requestedProjection: Schema,
    private val filter: FilterCompat.Filter,
    private val rowConvert: (GenericRecord) => T,
) extends BitlapReader[T] {

    private val conf = fs.newConf()
    private val inputsIt = inputs.iterator
    private var currentRow: T = _
    private var currentRows: mutable.ArrayDeque[T] = new mutable.ArrayDeque()
    private var reader: ParquetReader[GenericRecord] = _

    override def read(): T = {
        if (hasNext()) {
            return next()
        }
        return null.asInstanceOf[T]
    }

    override def read(limit: Int): List[T] = {
        val result = ListBuffer[T]()
        var count = 0
        while (hasNext() && count < limit) {
            result += next()
            count += 1
        }
        return result.toList
    }

    override def hasNext(): Boolean = {
        if (this.currentRows.nonEmpty) {
            return true
        }
        var raw = this.reader.read()
        while (raw != null) {
            this.currentRow = this.rowConvert(raw)
            if (this.currentRow != null) {
                this.currentRows.prepend(this.currentRow)
                return true
            }
            raw = this.reader.read()
        }
        initReader()
        return this.reader != null && hasNext()
    }

    override def next(): T = {
        return this.currentRows.removeLast()
    }

    private def initReader(): Unit = {
        if (this.reader != null) {
            scala.util.Using.resource(this.reader)
            this.reader = null
        }
        if (inputsIt.hasNext) {
            AvroReadSupport.setAvroReadSchema(conf, schema)
            AvroReadSupport.setRequestedProjection(conf, requestedProjection)
            val file = HadoopInputFile.fromPath(inputsIt.next(), conf)
            this.reader = AvroParquetReader.builder[GenericRecord](file)
                .withFilter(filter)
                .withConf(conf)
                .build()
        }
    }

    override def close(): Unit = {
      scala.util.Using.resource(this.reader)
        this.reader = null
    }
}

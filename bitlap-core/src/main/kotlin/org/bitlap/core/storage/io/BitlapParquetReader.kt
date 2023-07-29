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
import org.bitlap.core.utils.Hcfs.newConf

/**
 * Desc: bitlap metric writer implemented by parquet
 */
class BitlapParquetReader<T>(
    private val fs: FileSystem,
    private val inputs: List<Path>,
    private val schema: Schema,
    private val requestedProjection: Schema,
    private val filter: FilterCompat.Filter,
    private val rowConvert: (GenericRecord) -> T?,
) : BitlapReader<T> {

    private val conf = fs.newConf()
    private val inputsIt = inputs.iterator()
    private var currentRow: T? = null
    private var currentRows: ArrayDeque<T> = ArrayDeque(10)
    private var reader: ParquetReader<GenericRecord>? = null

    override fun read(): T? {
        if (hasNext()) {
            return next()
        }
        return null
    }

    override fun read(limit: Int): List<T> {
        val result = mutableListOf<T>()
        var count = 0
        while (hasNext() && count < limit) {
            result.add(next())
            count++
        }
        return result
    }

    override fun hasNext(): Boolean {
        if (this.currentRows.isNotEmpty()) {
            return true
        }
        var raw = this.reader?.read()
        while (raw != null) {
            this.currentRow = this.rowConvert.invoke(raw)
            if (this.currentRow != null) {
                this.currentRows.addFirst(this.currentRow!!)
                return true
            }
            raw = this.reader?.read()
        }
        initReader()
        return this.reader != null && hasNext()
    }

    override fun next(): T {
        return this.currentRows.removeLast()
    }

    private fun initReader() {
        if (this.reader != null) {
            this.reader.use { }
            this.reader = null
        }
        if (inputsIt.hasNext()) {
            AvroReadSupport.setAvroReadSchema(conf, schema)
            AvroReadSupport.setRequestedProjection(conf, requestedProjection)
            val file = HadoopInputFile.fromPath(inputsIt.next(), conf)
            this.reader = AvroParquetReader.builder<GenericRecord>(file)
                .withFilter(filter)
                .withConf(conf)
                .build()
        }
    }

    override fun close() {
        this.reader.use { }
        this.reader = null
    }
}

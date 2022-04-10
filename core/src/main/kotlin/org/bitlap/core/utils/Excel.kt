/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.utils

import cn.hutool.core.io.FileUtil
import cn.hutool.core.text.csv.CsvReadConfig
import cn.hutool.core.text.csv.CsvReader
import cn.hutool.poi.excel.ExcelUtil
import java.io.InputStream

/**
 * excel utils
 */
object Excel {

    /**
     * read csv content from path
     */
    fun String.readCsv(): Pair<List<String>, List<List<Any?>>> {
        val file = FileUtil.file(this)
        if (!file.exists()) {
            throw IllegalArgumentException("Csv file $this does not exist.")
        }
        return file.inputStream().readCsv()
    }

    /**
     * read csv content from file input stream
     */
    fun InputStream.readCsv(): Pair<List<String>, List<List<Any?>>> {
        val reader = CsvReader(this.reader(), CsvReadConfig().setContainsHeader(true))
        return reader.use { r ->
            val data = r.read()
            data.header to data.rows.map { it.toList() }
        }
    }

    /**
     * read excel content from path
     */
    fun String.readExcel(): Pair<List<String>, List<List<Any?>>> {
        val file = FileUtil.file(this)
        if (!file.exists()) {
            throw IllegalArgumentException("Excel file $this does not exist.")
        }
        return file.inputStream().readExcel()
    }

    /**
     * read excel content from file input stream
     */
    fun InputStream.readExcel(): Pair<List<String>, List<List<Any?>>> {
        return ExcelUtil.getReader(this).use { reader ->
            val header = reader.read(0, 0).first().map { it.toString() }
            header to reader.read(1)
        }
    }
}

package org.bitlap.common.utils

import cn.hutool.core.io.FileUtil
import cn.hutool.poi.excel.ExcelUtil
import java.io.InputStream

/**
 * excel utils
 */
typealias HeaderChecker = ((List<String>) -> Boolean)?
object Excel {

    /**
     * read excel content from path
     */
    fun String.readExcel(headerChecker: HeaderChecker = null): List<List<Any?>> {
        val file = FileUtil.file(this)
        if (!file.exists()) {
            throw IllegalArgumentException("Excel file $this does not exist.")
        }
        return file.inputStream().readExcel(headerChecker)
    }

    /**
     * read excel content from file input stream
     */
    fun InputStream.readExcel(headerChecker: HeaderChecker = null): List<List<Any?>> {
        return ExcelUtil.getReader(this).use { reader ->
            if (headerChecker != null) {
                val header = reader.read(0, 0).first().map { it.toString() }
                PreConditions.checkExpression(headerChecker.invoke(header), msg = "Header $header is invalid.")
            }
            reader.read(1)
        }
    }
}

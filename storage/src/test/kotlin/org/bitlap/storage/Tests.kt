package org.bitlap.storage

import org.apache.carbondata.sdk.file.CarbonReader
import org.apache.carbondata.sdk.file.CarbonWriter
import org.apache.commons.io.FileUtils
import java.io.File
import java.util.*

/**
 * Desc:
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/11/26
 */
fun main() {

    val path = "/tmp/carbon/test/testWriteFilesBuildWithJsonSchema"
    FileUtils.deleteDirectory(File(path))

    val schema = "[{name:string},{age:int},{height:double}]"
    val builder = CarbonWriter
        .builder()
        .outputPath(path)
        .withCsvInput(schema)
        .writtenBy("testWriteFilesBuildWithJsonSchema")
    val writer = builder.build()
    for (i in 0..9) {
        writer.write(arrayOf(
            "robot" + i % 10, (i % 3000000).toString(), (i.toDouble() / 2).toString()))
    }
    writer.close()

    val carbonReader: CarbonReader<*> = CarbonReader.builder(path)
        .projection(arrayOf("name", "age"))
        .build<Any>()
    var i = 0
    while (carbonReader.hasNext()) {
        val row = carbonReader.readNextRow() as Array<Any>
        println(Arrays.toString(row))
        i++
    }
    carbonReader.close()
}
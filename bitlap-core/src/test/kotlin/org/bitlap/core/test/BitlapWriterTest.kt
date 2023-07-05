/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.test

import org.bitlap.core.BitlapContext
import org.bitlap.core.mdm.BitlapEventWriter
import org.bitlap.core.test.base.BaseLocalFsTest
import org.bitlap.core.test.base.SqlChecker

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/3/5
 */
class BitlapWriterTest : BaseLocalFsTest(), SqlChecker {
    init {

        "test csv writer" {
            val (dbName, tableName) = randomDBTable()
            sql("create table $dbName.$tableName")
            val table = BitlapContext.catalog.getTable(tableName, dbName)
            val writer = BitlapEventWriter(table, hadoopConf)
            val input = BitlapWriterTest::class.java.classLoader.getResourceAsStream("simple_data.csv")!!
            writer.writeCsv(input)
            checkRows(
                "select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $table where _time >= 100 group by _time",
                listOf(listOf(100, 4, 12, 3), listOf(200, 4, 12, 3))
            )
            checkRows(
                "select sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $table where _time >= 100",
                listOf(listOf(8, 24, 3))
            )
        }
    }
}

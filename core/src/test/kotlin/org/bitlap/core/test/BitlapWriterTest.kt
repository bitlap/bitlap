package org.bitlap.core.test

import io.kotest.matchers.shouldBe
import org.bitlap.common.data.Dimension
import org.bitlap.common.data.Entity
import org.bitlap.common.data.Event
import org.bitlap.common.data.Metric
import org.bitlap.core.BitlapContext
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.mdm.BitlapReader
import org.bitlap.core.mdm.BitlapWriter
import org.bitlap.core.mdm.model.Query
import org.bitlap.core.mdm.model.QueryMetric
import org.bitlap.core.mdm.model.QueryTime
import org.bitlap.core.test.base.BaseLocalFsTest
import org.bitlap.core.test.base.SqlChecker
import org.joda.time.DateTime

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/3/5
 */
class BitlapWriterTest : BaseLocalFsTest(), SqlChecker {
    init {

        "test SimpleBitlapWriter" {
            val database = randomString()
            val table = randomString()
            BitlapContext.catalog.createTable(table, database, true)
            val writer = BitlapWriter(Table(database, table), conf, hadoopConf)

            val testTime = DateTime.parse("2021-01-01").millis
            val testTime2 = DateTime.parse("2021-01-02").millis
            writer.use {
                it.write(
                    listOf(
                        Event.of(testTime, Entity(1), Dimension("city" to "北京", "os" to "Mac"), Metric("pv", 2.0)),
                        Event.of(testTime, Entity(1), Dimension("city" to "北京", "os" to "Windows"), Metric("pv", 3.0)),
                        Event.of(testTime, Entity(2), Dimension("city" to "北京", "os" to "Mac"), Metric("pv", 5.0)),
                        Event.of(testTime, Entity(2), Dimension("city" to "北京", "os" to "Mac"), Metric("vv", 10.0))
                    )
                )
                it.write(
                    listOf(
                        Event.of(testTime2, Entity(1), Dimension("city" to "北京", "os" to "Mac"), Metric("pv", 20.0)),
                        Event.of(testTime2, Entity(1), Dimension("city" to "北京", "os" to "Windows"), Metric("pv", 30.0)),
                        Event.of(testTime2, Entity(2), Dimension("city" to "北京", "os" to "Mac"), Metric("pv", 50.0)),
                        Event.of(testTime2, Entity(2), Dimension("city" to "北京", "os" to "Mac"), Metric("vv", 100.0))
                    )
                )
            }

            val reader = BitlapReader()
            val rows = reader.use {
                it.read(Query(database, table, QueryTime(testTime2), "user", listOf(QueryMetric("pv"), QueryMetric("vv"))))
            }
            rows.size shouldBe 1
            val pv = rows.first().getBM("pv")
            pv.getCount() shouldBe 100
            pv.getCountUnique() shouldBe 2
        }

        "test excel writer" {
            val db = randomString()
            val table = randomString()
            sql("create table $db.$table")
            val writer = BitlapWriter(Table(db, table), conf, hadoopConf)
            val input = BitlapWriterTest::class.java.classLoader.getResourceAsStream("simple_data.xlsx")!!
            writer.writeExcel(input)
            checkRows(
                "select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $db.$table where _time >= 100 group by _time",
                listOf(listOf(100, 4, 12, 3), listOf(200, 4, 12, 3))
            )
            checkRows(
                "select sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $db.$table where _time >= 100",
                listOf(listOf(8, 24, 3))
            )
        }
    }
}

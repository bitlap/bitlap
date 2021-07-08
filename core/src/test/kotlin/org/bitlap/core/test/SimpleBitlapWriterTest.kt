package org.bitlap.core.test

import io.kotest.matchers.shouldBe
import org.bitlap.common.bitmap.CBM
import org.bitlap.core.BitlapContext
import org.bitlap.core.io.DefaultBitlapReader
import org.bitlap.core.io.SimpleBitlapWriter
import org.bitlap.core.model.SimpleRow
import org.bitlap.core.model.query.Query
import org.bitlap.core.model.query.QueryMetric
import org.bitlap.core.model.query.QueryTime
import org.bitlap.core.test.base.BaseLocalFsTest
import org.joda.time.DateTime

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/3/5
 */
class SimpleBitlapWriterTest : BaseLocalFsTest() {
    init {

        "test SimpleBitlapWriter" {
            val dsName = "test_datasource"
            BitlapContext.dataSourceManager.createDataSource(dsName, true)
            val writer = SimpleBitlapWriter(dsName)

            val testTime = DateTime.parse("2021-01-01").millis
            val testTime2 = DateTime.parse("2021-01-02").millis
            writer.use {
                it.write(
                    listOf(
                        SimpleRow(testTime, mapOf("user" to 1), mapOf("city" to "北京", "os" to "Mac"), mapOf("pv" to 2.0)),
                        SimpleRow(testTime, mapOf("user" to 1), mapOf("city" to "北京", "os" to "Windows"), mapOf("pv" to 3.0)),
                        SimpleRow(testTime, mapOf("user" to 2), mapOf("city" to "北京", "os" to "Mac"), mapOf("pv" to 5.0)),
                        SimpleRow(testTime, mapOf("user" to 2), mapOf("city" to "北京", "os" to "Mac"), mapOf("vv" to 10.0))
                    )
                )
                it.write(
                    listOf(
                        SimpleRow(testTime2, mapOf("user" to 1), mapOf("city" to "北京", "os" to "Mac"), mapOf("pv" to 20.0)),
                        SimpleRow(testTime2, mapOf("user" to 1), mapOf("city" to "北京", "os" to "Windows"), mapOf("pv" to 30.0)),
                        SimpleRow(testTime2, mapOf("user" to 2), mapOf("city" to "北京", "os" to "Mac"), mapOf("pv" to 50.0)),
                        SimpleRow(testTime2, mapOf("user" to 2), mapOf("city" to "北京", "os" to "Mac"), mapOf("vv" to 100.0))
                    )
                )
            }

            val reader = DefaultBitlapReader()
            val rows = reader.use {
                it.read(Query(dsName, QueryTime(testTime2), "user", listOf(QueryMetric("pv"), QueryMetric("vv"))))
            }
            rows.size shouldBe 1
            val pv = rows.first().getBM("pv")
            pv.getCount() shouldBe 100
            pv.getCountUnique() shouldBe 2
        }
    }
}

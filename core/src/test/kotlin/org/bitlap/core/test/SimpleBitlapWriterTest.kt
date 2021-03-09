package org.bitlap.core.test

import org.bitlap.core.DataSourceManager
import org.bitlap.core.model.SimpleRow
import org.bitlap.core.test.base.BaseLocalFsTest
import org.bitlap.core.writer.SimpleBitlapWriter
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
            DataSourceManager.createDataSource(dsName, true)
            val writer = SimpleBitlapWriter(dsName)

            val testTime = DateTime.parse("2021-01-01").millis
            writer.use {
                it.write(
                    listOf(
                        SimpleRow(testTime, mapOf("user" to 1), mapOf("city" to "北京", "os" to "Mac"), mapOf("pv" to 2.0)),
                        SimpleRow(testTime, mapOf("user" to 1), mapOf("city" to "北京", "os" to "Windows"), mapOf("pv" to 3.0)),
                        SimpleRow(testTime, mapOf("user" to 2), mapOf("city" to "北京", "os" to "Mac"), mapOf("pv" to 5.0))
                    )
                )
            }
        }
    }
}

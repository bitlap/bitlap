package org.bitlap.core.test

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.bitlap.common.error.BitlapException
import org.bitlap.core.DataSourceManager
import org.bitlap.core.test.base.BaseLocalFsTest

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/25
 */
class DataSourceManagerTest : BaseLocalFsTest() {
    init {
        "test DataSource create" {
            val testName = "test_datasource"
            val ds = DataSourceManager
            ds.createDataSource(testName)
            ds.createDataSource(testName, true)
            // get datasource
            shouldThrow<BitlapException> { ds.getDataSource("xxx") }
            ds.getDataSource(testName).createTime shouldNotBe null
        }
    }
}

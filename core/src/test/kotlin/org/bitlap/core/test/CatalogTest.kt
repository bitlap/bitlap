package org.bitlap.core.test

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.bitlap.common.exception.BitlapException
import org.bitlap.core.BitlapContext
import org.bitlap.core.test.base.BaseLocalFsTest

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/25
 */
class CatalogTest : BaseLocalFsTest() {
    init {

        "test Schema" {
            val testSchema = "test_schema"
            val catalog = BitlapContext.catalog
            catalog.createSchema(testSchema)
            catalog.dropSchema(testSchema)
            catalog.createSchema(testSchema, true)
            shouldThrow<BitlapException> { catalog.createSchema(testSchema) }
            catalog.renameSchema(testSchema, "test_schema_to")
            catalog.renameSchema("test_schema_to", testSchema)
            catalog.getSchema(testSchema) shouldBe testSchema
        }

        "test DataSource create" {
            val testName = "test_datasource"
            val catalog = BitlapContext.catalog
            catalog.createDataSource(testName)
            catalog.createDataSource(testName, ifNotExists = true)
            // get datasource
            shouldThrow<BitlapException> { catalog.getDataSource("xxx") }
            val getDS = catalog.getDataSource(testName)
            getDS.name shouldBe testName
            getDS.createTime shouldNotBe null
        }
    }
}

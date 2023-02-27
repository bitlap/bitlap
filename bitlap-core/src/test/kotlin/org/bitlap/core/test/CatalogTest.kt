/* Copyright (c) 2023 bitlap.org */
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

        "test database" {
            val testDatabase = randomDatabase()
            val catalog = BitlapContext.catalog
            catalog.createDatabase(testDatabase)
            catalog.dropDatabase(testDatabase)
            catalog.createDatabase(testDatabase, true)
            shouldThrow<BitlapException> { catalog.createDatabase(testDatabase) }
            catalog.renameDatabase(testDatabase, "test_database_to")
            catalog.renameDatabase("test_database_to", testDatabase)
            catalog.getDatabase(testDatabase).name shouldBe testDatabase
            catalog.dropDatabase(testDatabase, true)
        }

        "test table create" {
            val testName = randomTable()
            val catalog = BitlapContext.catalog
            catalog.createTable(testName)
            catalog.createTable(testName, ifNotExists = true)
            // get table
            shouldThrow<BitlapException> { catalog.getTable("xxx") }
            val getTable = catalog.getTable(testName)
            getTable.name shouldBe testName
            getTable.createTime shouldNotBe null
            catalog.dropTable(testName)
        }

        "test use database and show current_database" {
            val testName = randomTable()
            val catalog = BitlapContext.catalog
            catalog.createDatabase(testName)
            catalog.useDatabase(testName) shouldBe true
            catalog.dropDatabase(testName)
            shouldThrow<BitlapException> { catalog.useDatabase(testName) }

            val testName2 = randomTable()
            val catalog2 = BitlapContext.catalog
            catalog2.createDatabase(testName2)
            catalog2.useDatabase(testName2) shouldBe true
            catalog2.showCurrentDatabase() shouldBe testName2
        }
    }
}

/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.test

import org.bitlap.common.exception.BitlapException
import org.bitlap.core.BitlapContext
import org.bitlap.core.test.base.BaseLocalFsTest

class CatalogTest extends BaseLocalFsTest {

  test("test database") {
    val testDatabase = randomDatabase()
    val catalog      = BitlapContext.catalog
    catalog.createDatabase(testDatabase)
    catalog.dropDatabase(testDatabase)
    catalog.createDatabase(testDatabase, true)
    assertThrows[BitlapException] { catalog.createDatabase(testDatabase) }
    catalog.renameDatabase(testDatabase, "test_database_to")
    catalog.renameDatabase("test_database_to", testDatabase)
    catalog.getDatabase(testDatabase).name shouldBe testDatabase
    catalog.dropDatabase(testDatabase, true)
  }

  test("test table create") {
    val testName = randomTable()
    val catalog  = BitlapContext.catalog
    catalog.createTable(testName)
    catalog.createTable(testName, ifNotExists = true)
    // get table
    assertThrows[BitlapException] { catalog.getTable("xxx") }
    val getTable = catalog.getTable(testName)
    getTable.name shouldBe testName
    getTable.createTime should be > 0L
    catalog.dropTable(testName)
  }
}

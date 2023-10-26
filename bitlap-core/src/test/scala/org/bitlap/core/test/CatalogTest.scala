/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

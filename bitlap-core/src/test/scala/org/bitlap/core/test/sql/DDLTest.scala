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
package org.bitlap.core.test.sql

import org.bitlap.common.exception.BitlapException
import org.bitlap.core.catalog.metadata.Account.DEFAULT_USER
import org.bitlap.core.catalog.metadata.Database.DEFAULT_DATABASE
import org.bitlap.core.test.base.BaseLocalFsTest
import org.bitlap.core.test.base.SqlChecker
import org.bitlap.core.test.base.SqlSession

class DDLTest extends BaseLocalFsTest with SqlChecker {

  test("common user auth ddl statements") {
    val testUser = randomUser()
    // auth root default password
    sql(s"auth root") shouldEqual List(List(true))

    sql(s"create user if not exists $testUser identified by 'password'") shouldEqual List(List(true))
    sql(s"auth $testUser 'password'") shouldEqual List(List(true))
    sql(s"drop user $testUser") shouldEqual List(List(true))
  }

  test("common user pwd ddl statements") {
    val testUser = randomUser()
    // create
    sql(s"create user $testUser") shouldEqual List(List(true))
    assertThrows[BitlapException] {
      sql(s"create user $testUser identified by 'password'")
    }
    sql(s"create user if not exists $testUser identified by 'password'") shouldEqual List(List(false))
    // show
    sql(s"show users").result should contain(List(DEFAULT_USER))
    sql(s"show users").result should contain(List(testUser))
    // drop
    sql(s"drop user $testUser") shouldEqual List(List(true))
    sql(s"show users").result should not contain List(testUser)
    assertThrows[BitlapException] {
      sql(s"drop user $testUser")
    }
  }

  test("common user ddl statements") {
    val testUser = randomUser()
    // create
    sql(s"create user $testUser") shouldEqual List(List(true))
    assertThrows[BitlapException] {
      sql(s"create user $testUser")
    }
    sql(s"create user if not exists $testUser") shouldEqual List(List(false))
    // show
    sql(s"show users").result should contain(List(DEFAULT_USER))
    sql(s"show users").result should contain(List(testUser))
    // drop
    sql(s"drop user $testUser") shouldEqual List(List(true))
    sql(s"show users").result should not contain List(testUser)
    assertThrows[BitlapException] {
      sql(s"drop user $testUser")
    }
  }

  test("common database ddl statements") {
    val testDB = randomDatabase()
    // create
    sql(s"create database $testDB") shouldEqual List(List(true))
    assertThrows[BitlapException] { sql(s"create database $testDB") }
    sql(s"create database if not exists $testDB") shouldEqual List(List(false))
    // show
    sql(s"show databases").result should contain(List(DEFAULT_DATABASE))
    sql(s"show databases").result should contain(List(testDB))
    // drop
    sql(s"drop database $testDB") shouldEqual List(List(true))
    sql(s"show databases").result should not contain List(testDB)
    assertThrows[BitlapException] { sql(s"drop database $testDB") }
    sql(s"drop database if exists $testDB") shouldEqual List(List(false))
  }

  test("forbidden operation of default database") {
    assertThrows[BitlapException] { sql(s"create database $DEFAULT_DATABASE") }
    assertThrows[BitlapException] { sql(s"drop database $DEFAULT_DATABASE") }
  }

  test("common table ddl statements") {
    val testDB    = randomDatabase()
    val testTable = "test_table"
    // create
    sql(s"create database $testDB")
    sql(s"create table $testDB.$testTable")
    assertThrows[BitlapException] { sql(s"create table $testDB.$testTable") }
    sql(s"create table if not exists $testDB.$testTable")
    // show
    sql(s"show tables").size should be >= 0
    sql(s"show tables in $testDB").size shouldBe 1
    // drop
    sql(s"drop table $testDB.$testTable")
    assertThrows[BitlapException] { sql(s"drop table $testDB.$testTable") }
    sql(s"drop table if exists $testDB.$testTable")
    // show
    sql(s"show tables in $testDB").size shouldBe 0
  }

  test("test use statement") {
    val session     = SqlSession()
    val (db, table) = randomDBTable()
    sql(s"create table $db.$table", session)
    sql(s"use $db", session)
    checkRows(
      s"show current_database",
      List(List(db)),
      session
    )
    sql(s"use default", session)
    checkRows(
      s"show current_database",
      List(List(DEFAULT_DATABASE)),
      session
    )
  }
}

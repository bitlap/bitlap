/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.test.sql

import org.bitlap.common.exception.BitlapException
import org.bitlap.core.catalog.metadata.Database.DEFAULT_DATABASE
import org.bitlap.core.test.base.BaseLocalFsTest
import org.bitlap.core.test.base.SqlChecker
import org.bitlap.core.test.base.SqlSession

class DDLTest extends BaseLocalFsTest with SqlChecker {

  test("common database ddl statements") {
    val testDB = randomDatabase()
    // create
    sql(s"create database $testDB") shouldEqual List(List(true))
    assertThrows[BitlapException] { sql(s"create database $testDB") }
    sql(s"create database if not exists $testDB") shouldEqual List(List(false))
    // show
    sql(s"show databases").result should contain(DEFAULT_DATABASE)
    sql(s"show databases").result should contain(testDB)
    // drop
    sql(s"drop database $testDB") shouldEqual List(List(true))
    sql(s"show databases").result should not contain (testDB)
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

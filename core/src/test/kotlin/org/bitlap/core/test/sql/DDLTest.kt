/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.test.sql

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.collections.shouldNotContain
import io.kotest.matchers.ints.shouldBeGreaterThanOrEqual
import io.kotest.matchers.shouldBe
import org.bitlap.common.exception.BitlapException
import org.bitlap.core.Constants.DEFAULT_DATABASE
import org.bitlap.core.test.base.BaseLocalFsTest
import org.bitlap.core.test.base.SqlChecker

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/9/14
 */
class DDLTest : BaseLocalFsTest(), SqlChecker {

    init {
        "common database ddl statements" {
            val testDB = randomDatabase()
            // create
            sql("create database $testDB") shouldBe listOf(listOf(true))
            shouldThrow<BitlapException> { sql("create database $testDB") }
            sql("create database if not exists $testDB") shouldBe listOf(listOf(false))
            // show
            sql("show databases").result shouldContain listOf(DEFAULT_DATABASE)
            sql("show databases").result shouldContain listOf(testDB)
            // drop
            sql("drop database $testDB") shouldBe listOf(listOf(true))
            sql("show databases").result shouldNotContain listOf(testDB)
            shouldThrow<BitlapException> { sql("drop database $testDB") }
            sql("drop database if exists $testDB") shouldBe listOf(listOf(false))
        }

        "forbidden operation of default database" {
            shouldThrow<BitlapException> { sql("create database $DEFAULT_DATABASE") }
            shouldThrow<BitlapException> { sql("drop database $DEFAULT_DATABASE") }
        }

        "common table ddl statements" {
            val testDB = randomDatabase()
            val testTable = "test_table"
            // create
            sql("create database $testDB")
            sql("create table $testDB.$testTable")
            shouldThrow<BitlapException> { sql("create table $testDB.$testTable") }
            sql("create table if not exists $testDB.$testTable")
            // show
            sql("show tables").size shouldBeGreaterThanOrEqual 0
            sql("show tables in $testDB").size shouldBe 1
            // drop
            sql("drop table $testDB.$testTable")
            shouldThrow<BitlapException> { sql("drop table $testDB.$testTable") }
            sql("drop table if exists $testDB.$testTable")
            // show
            sql("show tables in $testDB").size shouldBe 0
        }
    }
}

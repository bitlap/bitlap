/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.test.sql

import org.bitlap.core.test.base.BaseLocalFsTest
import org.bitlap.core.test.base.SqlChecker
import org.bitlap.core.test.utils.FtpUtils

class LoadDataTest : BaseLocalFsTest(), SqlChecker {

    init {
        "test load data" {
            val (dbName, tableName) = randomDBTable()
            sql("create table $dbName.$tableName")
            sql("load data 'classpath:simple_data.csv' into table $dbName.$tableName")
            checkRows(
                "select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $dbName.$tableName where _time >= 100 group by _time",
                listOf(listOf(100, 4, 12, 3), listOf(200, 4, 12, 3))
            )
        }

        "test load data overwrite" {
            val (dbName, tableName) = randomDBTable()
            sql("create table $dbName.$tableName")
            sql("load data 'classpath:simple_data.csv' overwrite table $dbName.$tableName")
            sql("load data 'classpath:simple_data.csv' overwrite table $dbName.$tableName")
            checkRows(
                "select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $dbName.$tableName where _time >= 100 group by _time",
                listOf(listOf(100, 4, 12, 3), listOf(200, 4, 12, 3))
            )
        }

        "test load data from http" {
            val (dbName, tableName) = randomDBTable()
            sql("create table $dbName.$tableName")
            sql("load data 'http://ice-img.dreamylost.cn/files/simple_data.csv' overwrite table $dbName.$tableName")
            checkRows(
                "select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $dbName.$tableName where _time >= 100 group by _time",
                listOf(listOf(100, 4, 12, 3), listOf(200, 4, 12, 3))
            )
        }

        "test load data from ftp" {
            // 1. start ftp server
            val (server, port) = FtpUtils.start("${localFS.workingDirectory.toUri().path}/target/test-classes")
            // 2. create table
            val (dbName, tableName) = randomDBTable()
            sql("create table $dbName.$tableName")
            // 3. load data
            sql("load data 'ftp://bitlap:bitlap@127.0.0.1:$port/simple_data.csv' overwrite table $dbName.$tableName")
            checkRows(
                "select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $dbName.$tableName where _time >= 100 group by _time",
                listOf(listOf(100, 4, 12, 3), listOf(200, 4, 12, 3))
            )
            server.stop()
        }
    }
}

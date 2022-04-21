/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.test.sql

import org.bitlap.core.test.base.BaseLocalFsTest
import org.bitlap.core.test.base.SqlChecker

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
    }
}

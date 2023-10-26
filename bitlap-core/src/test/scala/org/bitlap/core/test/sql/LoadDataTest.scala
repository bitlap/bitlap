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

import org.bitlap.core.test.base.BaseLocalFsTest
import org.bitlap.core.test.base.SqlChecker
import org.bitlap.core.test.utils.FtpUtils

class LoadDataTest extends BaseLocalFsTest with SqlChecker {

  test("test load data") {
    val (dbName, tableName) = randomDBTable()
    sql(s"create table $dbName.$tableName")
    sql(s"load data 'classpath:simple_data.csv' into table $dbName.$tableName")
    checkRows(
      s"select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $dbName.$tableName where _time >= 100 group by _time",
      List(List(100, 4, 12, 3), List(200, 4, 12, 3))
    )
  }

  test("test load data overwrite") {
    val (dbName, tableName) = randomDBTable()
    sql(s"create table $dbName.$tableName")
    sql(s"load data 'classpath:simple_data.csv' overwrite table $dbName.$tableName")
    sql(s"load data 'classpath:simple_data.csv' overwrite table $dbName.$tableName")
    checkRows(
      s"select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $dbName.$tableName where _time >= 100 group by _time",
      List(List(100, 4, 12, 3), List(200, 4, 12, 3))
    )
  }

  test("test load data from http") {
    val (dbName, tableName) = randomDBTable()
    sql(s"create table $dbName.$tableName")
    sql(s"load data 'http://ice-img.dreamylost.cn/files/simple_data.csv' overwrite table $dbName.$tableName")
    checkRows(
      s"select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $dbName.$tableName where _time >= 100 group by _time",
      List(List(100, 4, 12, 3), List(200, 4, 12, 3))
    )
  }

  test("test load data from ftp") {
    // 1. start ftp server
    val (server, port) = FtpUtils.start(s"${localFS.getWorkingDirectory.toUri.getPath}/target/test-classes")
    // 2. create table
    val (dbName, tableName) = randomDBTable()
    sql(s"create table $dbName.$tableName")
    // 3. load data
    sql(s"load data 'ftp://bitlap:bitlap@127.0.0.1:$port/simple_data.csv' overwrite table $dbName.$tableName")
    checkRows(
      s"select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $dbName.$tableName where _time >= 100 group by _time",
      List(List(100, 4, 12, 3), List(200, 4, 12, 3))
    )
    server.stop()
  }
}

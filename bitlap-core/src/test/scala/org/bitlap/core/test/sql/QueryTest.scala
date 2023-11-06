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

import org.bitlap.common.data.Dimension
import org.bitlap.common.data.Entity
import org.bitlap.common.data.Event
import org.bitlap.common.data.Metric
import org.bitlap.common.exception.BitlapException
import org.bitlap.common.extension._
import org.bitlap.core.BitlapContext
import org.bitlap.core.mdm.BitlapEventWriter
import org.bitlap.core.test.base.BaseLocalFsTest
import org.bitlap.core.test.base.SqlChecker

class QueryTest extends BaseLocalFsTest with SqlChecker {

  test("simple query") {
    checkRows(s"select 1 as a, '2' as b, (1+2)*3 as c", List(List(1, "2", 9)))
    checkRows(
      s"select (a + cast(b as bigint) + c) as r from (select 1 as a, '2' as b, (1+2)*3 as c) t",
      List(List(12L))
    )
    checkRows(
      s"select sum(a) r from (select 1 as a union all select 2 as a union all select 3 as a) t",
      List(List(6))
    )
  }

  test("forbidden queries") {
    val (db, table) = randomDBTable()
    sql(s"create table $db.$table")
    assertThrows[BitlapException] {
      sql(s"select *, a, b from $db.$table") // star is forbidden
    }
    assertThrows[BitlapException] {
      sql(s"select a, b from $db.$table") // time filter is required
    }
    assertThrows[BitlapException] {
      sql(s"select a, b from $db.$table where _time = 123") // one aggregation metric is required
    }
    assertThrows[BitlapException] {
      sql(s"select count(1) cnt from $db.$table where _time = 123") // one special aggregation metric is required
    }
    assertThrows[BitlapException] {
      sql(s"select count(*) cnt from $db.$table where _time = 123") // one special aggregation metric is required
    }
  }

  test("query whatever you want") {
    val (db, table) = randomDBTable()
    sql(s"create table $db.$table")
    checkRows(
      s"select sum(pv) pv, count(distinct pv) uv, sum(xx) xx from $db.$table where _time = 100",
      List(List(0, 0, 0))
    )
  }

  test("only metrics query with one dimension time") {
    // System.setProperty("calcite.debug", "true")
    val (db, table) = randomDBTable()
    sql(s"create table $db.$table")
    prepareTestData(db, table, 100L)
    prepareTestData(db, table, 200L)
    checkRows(
      s"select count(vv) cvv, count(pv) cpv from $db.$table where _time = 100",
      List(List(4, 4))
    )
    checkRows(
      s"select count(vv) cvv, count(pv) cpv from $db.$table where 100 = _time",
      List(List(4, 4))
    )
    checkRows(
      s"select count(a) as a, count(distinct a) as a_dis, sum(b) as b from $db.$table where _time = 123",
      List(List(0, 0, 0))
    )
    checkRows(
      s"select sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $db.$table where _time = 100",
      List(List(4.0, 12.0, 3L))
    )
    checkRows(
      s"select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $db.$table where _time = 100 group by _time",
      List(List(100, 4, 12, 3))
    )
    checkRows(
      s"select sum(vv) as vv, sum(pv) as pv, _time, count(distinct pv) as uv from $db.$table where _time = 100 group by _time",
      List(List(4, 12, 100, 3))
    )
    checkRows(
      s"""
         |select
         |                  sum(vv) vv, sum(pv) pv
         |                from (
         |                  select sum(vv) as vv, sum(pv) as pv, _time, count(distinct pv) as uv
         |                  from $db.$table
         |                  where _time = 100
         |                  group by _time
         |                ) t
         |""".stripMargin,
      List(List(4, 12))
    )
    checkRows(
      s"select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $db.$table where _time >= 100 group by _time",
      List(List(100, 4, 12, 3), List(200, 4, 12, 3))
    )
    checkRows(
      s"select sum(vv) as vv, sum(pv) as pv, _time, count(distinct pv) as uv from $db.$table where _time >= 100 group by _time",
      List(List(4, 12, 100, 3), List(4, 12, 200, 3))
    )
    checkRows(
      s"select sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $db.$table where _time >= 100",
      List(List(8, 24, 3))
    )
    checkRows(
      s"select sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $db.$table where _time between 100 and 200",
      List(List(8, 24, 3))
    )
  }

  test("only metrics query with one complex dimension time") {
    val (db, table) = randomDBTable()
    sql(s"create table $db.$table")
    prepareTestData(db, table, 100L)
    prepareTestData(db, table, 200L)
    checkRows(
      s"""
         |select if (_time > 0, 'ALL', '0') _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv
         |from $db.$table
         |where _time >= 100
         |group by if (_time > 0, 'ALL', '0')
         |""".stripMargin,
      List(List("ALL", 8, 24, 3))
    )
    checkRows(
      s"""
         |select date_format(_time, 'yyyy-MM-dd') dt, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv
         |from $db.$table
         |where _time >= 100
         |group by date_format(_time, 'yyyy-MM-dd')
         |""".stripMargin,
      List(List("1970-01-01", 8, 24, 3))
    )
    checkRows(
      s"""
         |select date_format(_time, 'yyyy-MM-dd') dt, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv
         |from $db.$table
         |where _time >= 100
         |group by _time
         |""".stripMargin,
      List(List("1970-01-01", 4, 12, 3), List("1970-01-01", 4, 12, 3))
    )
  }

  test("only metrics query with one dimension that is not time") {
    val (db, table) = randomDBTable()
    sql(s"create table $db.$table")
    prepareTestData(db, table, 100L)
    prepareTestData(db, table, 200L)
    checkRows(
      s"select sum(vv) as vv, sum(pv) as pv from $db.$table where _time = 100 and os = 'Mac'",
      List(List(3, 9))
    )
    checkRows(
      s"select sum(vv) as vv, sum(pv) as pv from $db.$table where _time = 100 and os = 'Mac000'",
      List(List(0, 0))
    )
    checkRows(
      s"select sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $db.$table where _time = 100 and os = 'Mac'",
      List(List(3, 9, 3))
    )
    checkRows(
      s"""
         |select sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv
         |from $db.$table
         |where _time = 100 and os = 'Mac' and lower(os) = 'mac'
         |""".stripMargin,
      List(List(3, 9, 3))
    )
    checkRows(
      s"""
         |select sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv
         |from $db.$table
         |where _time = 100 and os = 'Mac' and lower(os) = 'xxx'
         |""".stripMargin,
      List(List(0, 0, 0))
    )
    checkRows(
      s"""
         |select os, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv
         |from $db.$table
         |where _time = 100 and os = 'Mac'
         |group by os
         |""".stripMargin,
      List(List("Mac", 3, 9, 3))
    )
    checkRows(
      s"""
         |select lower(os) os, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv
         |from $db.$table
         |where _time = 100 and os = 'Mac'
         |group by lower(os)
         |""".stripMargin,
      List(List("mac", 3, 9, 3))
    )
    checkRows(
      s"""
         |select os, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv
         |from $db.$table
         |where _time = 100 and (os = 'Mac' or os = 'Mac2' or os = '中文')
         |group by os
         |""".stripMargin,
      List(List("Mac", 3, 9, 3))
    )
    checkRows(
      s"""
         |select os, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv
         |from $db.$table
         |where _time = 100 and os in ('Mac', 'Mac2', '中文')
         |group by os
         |""".stripMargin,
      List(List("Mac", 3, 9, 3))
    )
  }

  test("only metrics query with one dimension with time") {
    val (db, table) = randomDBTable()
    sql(s"create table $db.$table")
    prepareTestData(db, table, 100L)
    prepareTestData(db, table, 200L)
    checkRows(
      s"""
         |select _time, os, sum(vv) as vv, sum(pv) as pv, count(distinct vv) uv
         |from $db.$table
         |where _time >= 100
         |group by _time, os
         |order by _time, os
         |""".stripMargin,
      List(
        List(100, "Mac", 3, 9, 3),
        List(100, "Windows", 1, 3, 1),
        List(200, "Mac", 3, 9, 3),
        List(200, "Windows", 1, 3, 1)
      )
    )
    checkRows(
      s"""
         |select _time, lower(os) os, sum(vv) as vv, sum(pv) as pv, count(distinct vv) uv
         |from $db.$table
         |where _time >= 100
         |group by _time, lower(os)
         |order by _time, os
         |""".stripMargin,
      List(
        List(100, "mac", 3, 9, 3),
        List(100, "windows", 1, 3, 1),
        List(200, "mac", 3, 9, 3),
        List(200, "windows", 1, 3, 1)
      )
    )
  }

  test("metrics query with more dimensions") {
    val (db, table) = randomDBTable()
    sql(s"create table $db.$table")
    prepareTestData(db, table, 100L)
    prepareTestData(db, table, 200L)
    checkRows(
      s"""
         |select _time, city, os, sum(vv) as vv, sum(pv) as pv, count(distinct vv) uv
         |from $db.$table
         |where _time >= 100
         |group by _time, city, os
         |order by _time, city, os
         |""".stripMargin,
      List(
        List(100, "北京", "Mac", 3, 9, 3),
        List(100, "北京", "Windows", 1, 3, 1),
        List(200, "北京", "Mac", 3, 9, 3),
        List(200, "北京", "Windows", 1, 3, 1)
      )
    )
    checkRows(
      s"""
         |select city, os, sum(vv) as vv, sum(pv) as pv, count(distinct vv) uv
         |from $db.$table
         |where _time >= 100
         |group by city, os
         |order by city, os
         |""".stripMargin,
      List(
        List("北京", "Mac", 6, 18, 3),
        List("北京", "Windows", 2, 6, 1)
      )
    )
    checkRows(
      s"""
         |select city, oo, sum(vv) as vv, sum(pv) as pv, count(distinct vv) uv
         |from $db.$table
         |where _time >= 100
         |group by city, oo
         |order by city, oo
         |""".stripMargin,
      List(
        List("北京", null, 8, 24, 3)
      )
    )
    checkRows(
      s"""
         |select _time, city, o1, o2, o3, sum(vv) as vv, sum(pv) as pv, count(distinct vv) uv
         |from $db.$table
         |where _time >= 100
         |group by _time, city, o1, o2, o3
         |order by _time, city, o1, o2, o3
         |""".stripMargin,
      List(
        List(100, "北京", null, null, null, 4, 12, 3),
        List(200, "北京", null, null, null, 4, 12, 3)
      )
    )
  }

  private def prepareTestData(database: String, tableName: String, time: Long): Unit = {
    val table  = BitlapContext.catalog.getTable(tableName, database)
    val writer = BitlapEventWriter(table, hadoopConf)
    writer.use { it =>
      it.write(
        List(
          Event.of(time, Entity(1, "ENTITY"), Dimension(Map("city" -> "北京", "os" -> "Mac")), Metric("vv", 1.0)),
          Event.of(time, Entity(1, "ENTITY"), Dimension(Map("city" -> "北京", "os" -> "Mac")), Metric("pv", 2.0)),
          Event
            .of(time, Entity(1, "ENTITY"), Dimension(Map("city" -> "北京", "os" -> "Windows")), Metric("vv", 1.0)),
          Event
            .of(time, Entity(1, "ENTITY"), Dimension(Map("city" -> "北京", "os" -> "Windows")), Metric("pv", 3.0)),
          Event.of(time, Entity(2, "ENTITY"), Dimension(Map("city" -> "北京", "os" -> "Mac")), Metric("vv", 1.0)),
          Event.of(time, Entity(2, "ENTITY"), Dimension(Map("city" -> "北京", "os" -> "Mac")), Metric("pv", 5.0)),
          Event.of(time, Entity(3, "ENTITY"), Dimension(Map("city" -> "北京", "os" -> "Mac")), Metric("vv", 1.0)),
          Event.of(time, Entity(3, "ENTITY"), Dimension(Map("city" -> "北京", "os" -> "Mac")), Metric("pv", 2.0))
        )
      )
    }
  }
}

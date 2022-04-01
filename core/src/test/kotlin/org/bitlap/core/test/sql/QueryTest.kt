/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.test.sql

import io.kotest.assertions.throwables.shouldThrow
import org.bitlap.common.data.Dimension
import org.bitlap.common.data.Entity
import org.bitlap.common.data.Event
import org.bitlap.common.data.Metric
import org.bitlap.common.exception.BitlapException
import org.bitlap.core.BitlapContext
import org.bitlap.core.mdm.BitlapWriter
import org.bitlap.core.test.base.BaseLocalFsTest
import org.bitlap.core.test.base.SqlChecker

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/9/14
 */
class QueryTest : BaseLocalFsTest(), SqlChecker {

    init {

        "simple query" {
            checkRows("select 1 as a, '2' as b, (1+2)*3 as c", listOf(listOf(1, "2", 9)))
            checkRows(
                "select (a + cast(b as bigint) + c) as r from (select 1 as a, '2' as b, (1+2)*3 as c) t",
                listOf(listOf(12L))
            )
            checkRows(
                "select sum(a) r from (select 1 as a union all select 2 as a union all select 3 as a) t",
                listOf(listOf(6))
            )
        }

        "forbidden queries" {
            val (db, table) = randomDBTable()
            sql("create table $db.$table")
            shouldThrow<BitlapException> {
                sql("select *, a, b from $db.$table") // star is forbidden
            }
            shouldThrow<BitlapException> {
                sql("select a, b from $db.$table") // time filter is required
            }
            shouldThrow<BitlapException> {
                sql("select a, b from $db.$table where _time = 123") // one aggregation metric is required
            }
            shouldThrow<BitlapException> {
                sql("select count(1) cnt from $db.$table where _time = 123") // one special aggregation metric is required
            }
            shouldThrow<BitlapException> {
                sql("select count(*) cnt from $db.$table where _time = 123") // one special aggregation metric is required
            }
        }

        "query whatever you want" {
            val (db, table) = randomDBTable()
            sql("create table $db.$table")
            checkRows(
                "select sum(pv) pv, count(distinct pv) uv, sum(xx) xx from $db.$table where _time = 100",
                listOf(listOf(0, 0, 0))
            )
        }

        "only metrics query with one dimension time" {
            // System.setProperty("calcite.debug", "true")
            val (db, table) = randomDBTable()
            sql("create table $db.$table")
            prepareTestData(db, table, 100L)
            prepareTestData(db, table, 200L)
            checkRows(
                "select count(vv) cvv, count(pv) cpv from $db.$table where _time = 100",
                listOf(listOf(4, 4))
            )
            checkRows(
                "select count(vv) cvv, count(pv) cpv from $db.$table where 100 = _time",
                listOf(listOf(4, 4))
            )
            checkRows(
                "select count(a) as a, count(distinct a) as a_dis, sum(b) as b from $db.$table where _time = 123",
                listOf(listOf(0, 0, 0))
            )
            checkRows(
                "select sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $db.$table where _time = 100",
                listOf(listOf(4.0, 12.0, 3L))
            )
            checkRows(
                "select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $db.$table where _time = 100 group by _time",
                listOf(listOf(100, 4, 12, 3))
            )
            checkRows(
                "select sum(vv) as vv, sum(pv) as pv, _time, count(distinct pv) as uv from $db.$table where _time = 100 group by _time",
                listOf(listOf(4, 12, 100, 3))
            )
            checkRows(
                """
                select 
                  sum(vv) vv, sum(pv) pv
                from (
                  select sum(vv) as vv, sum(pv) as pv, _time, count(distinct pv) as uv 
                  from $db.$table 
                  where _time = 100
                  group by _time
                ) t
                """.trimIndent(),
                listOf(listOf(4, 12))
            )
            checkRows(
                "select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $db.$table where _time >= 100 group by _time",
                listOf(listOf(100, 4, 12, 3), listOf(200, 4, 12, 3))
            )
            checkRows(
                "select sum(vv) as vv, sum(pv) as pv, _time, count(distinct pv) as uv from $db.$table where _time >= 100 group by _time",
                listOf(listOf(4, 12, 100, 3), listOf(4, 12, 200, 3))
            )
            checkRows(
                "select sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $db.$table where _time >= 100",
                listOf(listOf(8, 24, 3))
            )
            checkRows(
                "select sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $db.$table where _time between 100 and 200",
                listOf(listOf(8, 24, 3))
            )
        }

        "only metrics query with one complex dimension time" {
            val (db, table) = randomDBTable()
            sql("create table $db.$table")
            prepareTestData(db, table, 100L)
            prepareTestData(db, table, 200L)
            checkRows(
                """
                    select if (_time > 0, 'ALL', '0') _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv 
                    from $db.$table 
                    where _time >= 100 
                    group by if (_time > 0, 'ALL', '0')
                """.trimIndent(),
                listOf(listOf("ALL", 8, 24, 3))
            )
        }

        "only metrics query with one dimension that is not time" {
            val (db, table) = randomDBTable()
            sql("create table $db.$table")
            prepareTestData(db, table, 100L)
            prepareTestData(db, table, 200L)
            checkRows(
                "select sum(vv) as vv, sum(pv) as pv from $db.$table where _time = 100 and os = 'Mac'",
                listOf(listOf(3, 9))
            )
            checkRows(
                "select sum(vv) as vv, sum(pv) as pv from $db.$table where _time = 100 and os = 'Mac000'",
                listOf(listOf(0, 0))
            )
            checkRows(
                "select sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $db.$table where _time = 100 and os = 'Mac'",
                listOf(listOf(3, 9, 3))
            )
            checkRows(
                """
                    select sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv
                    from $db.$table
                    where _time = 100 and os = 'Mac' and lower(os) = 'mac'
                """.trimIndent(),
                listOf(listOf(3, 9, 3))
            )
            checkRows(
                """
                    select sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv
                    from $db.$table
                    where _time = 100 and os = 'Mac' and lower(os) = 'xxx'
                """.trimIndent(),
                listOf(listOf(0, 0, 0))
            )
            checkRows(
                """
                    select os, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv
                    from $db.$table
                    where _time = 100 and os = 'Mac'
                    group by os
                """.trimIndent(),
                listOf(listOf("Mac", 3, 9, 3))
            )
            checkRows(
                """
                    select lower(os) os, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv
                    from $db.$table
                    where _time = 100 and os = 'Mac'
                    group by lower(os)
                """.trimIndent(),
                listOf(listOf("mac", 3, 9, 3))
            )
            checkRows(
                """
                    select os, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv
                    from $db.$table
                    where _time = 100 and (os = 'Mac' or os = 'Mac2' or os = '中文')
                    group by os
                """.trimIndent(),
                listOf(listOf("Mac", 3, 9, 3))
            )
            checkRows(
                """
                    select os, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv
                    from $db.$table
                    where _time = 100 and os in ('Mac', 'Mac2', '中文')
                    group by os
                """.trimIndent(),
                listOf(listOf("Mac", 3, 9, 3))
            )
        }
    }

    private fun prepareTestData(database: String, tableName: String, time: Long) {
        val table = BitlapContext.catalog.getTable(tableName, database)
        val writer = BitlapWriter(table, hadoopConf)
        writer.use {
            it.write(
                listOf(
                    Event.of(time, Entity(1), Dimension("city" to "北京", "os" to "Mac"), Metric("vv", 1.0)),
                    Event.of(time, Entity(1), Dimension("city" to "北京", "os" to "Mac"), Metric("pv", 2.0)),
                    Event.of(time, Entity(1), Dimension("city" to "北京", "os" to "Windows"), Metric("vv", 1.0)),
                    Event.of(time, Entity(1), Dimension("city" to "北京", "os" to "Windows"), Metric("pv", 3.0)),
                    Event.of(time, Entity(2), Dimension("city" to "北京", "os" to "Mac"), Metric("vv", 1.0)),
                    Event.of(time, Entity(2), Dimension("city" to "北京", "os" to "Mac"), Metric("pv", 5.0)),
                    Event.of(time, Entity(3), Dimension("city" to "北京", "os" to "Mac"), Metric("vv", 1.0)),
                    Event.of(time, Entity(3), Dimension("city" to "北京", "os" to "Mac"), Metric("pv", 2.0)),
                )
            )
        }
    }
}

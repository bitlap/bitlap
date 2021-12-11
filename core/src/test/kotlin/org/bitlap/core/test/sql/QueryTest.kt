package org.bitlap.core.test.sql

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import org.bitlap.common.data.Dimension
import org.bitlap.common.data.Entity
import org.bitlap.common.data.Event
import org.bitlap.common.data.Metric
import org.bitlap.common.exception.BitlapException
import org.bitlap.core.mdm.io.SimpleBitlapWriter
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
            sql("select 1 as a, '2' as b, (1+2)*3 as c") shouldBe listOf(listOf(1, "2", 9))
            sql("select (a + cast(b as bigint) + c) as r from (select 1 as a, '2' as b, (1+2)*3 as c) t") shouldBe listOf(listOf(12L))
            sql("select sum(a) r from (select 1 as a union all select 2 as a union all select 3 as a) t") shouldBe listOf(listOf(6))
        }

        "forbidden queries" {
            val db = randomString()
            val table = randomString()
            sql("create table $db.$table")
            shouldThrow<BitlapException> {
                sql("select *, a, b from $db.$table") // star is forbidden
            }
            shouldThrow<BitlapException> {
                sql("select a, b from $db.$table") // time filter is required
            }
            shouldThrow<BitlapException> {
                sql("select a, b from $db.$table where _time=123") // one aggregation metric is required
            }
        }

        "only metrics query with one dimension time" {
            val db = randomString()
            val table = randomString()
            sql("create table $db.$table")
            prepareTestData(db, table, 100L)
            prepareTestData(db, table, 200L)
            sql(
                """
                select count(a) as a, count(distinct a) as a_dis, sum(b) as b 
                from $db.$table
                where _time=123
                """.trimIndent()
            ).show() shouldBe listOf(listOf(0L, 0L, 0.0))
            sql(
                """
                select sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv 
                from $db.$table 
                where _time=100
                """.trimIndent()
            ) shouldBe listOf(listOf(3.0, 10.0, 2L))
            sql(
                """
                select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv 
                from $db.$table 
                where _time=100
                group by _time
                """.trimIndent()
            ) shouldBe listOf(listOf(100L, 3.0, 10.0, 2L))
            sql(
                """
                select sum(vv) as vv, sum(pv) as pv, _time, count(distinct pv) as uv 
                from $db.$table 
                where _time=100
                group by _time
                """.trimIndent()
            ) shouldBe listOf(listOf(3.0, 10.0, 100L, 2L))
            sql(
                """
                select
                  sum(vv) vv, sum(pv) pv
                from (
                  select sum(vv) as vv, sum(pv) as pv, _time, count(distinct pv) as uv 
                  from $db.$table 
                  where _time=100
                  group by _time
                ) t
                """.trimIndent()
            ) shouldBe listOf(listOf(3.0, 10.0))

        }

        "single metric query2" {
//            System.setProperty("calcite.debug", "true")
            val db = randomString()
            val table = randomString()
            sql("create table $db.$table")
            prepareTestData(db, table, 100L)
            prepareTestData(db, table, 200L)
//            sql("select sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $table where _time=100 and (c='123' or c='1234')").show()
//            sql("select sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $db.$table where _time>=100").show()
//            sql("select _time, sum(vv) as vv1, sum(pv) as pv, count(distinct pv) as uv from $db.$table where _time>=100 group by _time").show()
            sql("select sum(vv) from (select sum(vv) as vv, sum(pv) as pv, _time, count(distinct pv) as uv from $db.$table where _time>=100 group by _time ) t").show()
        }
    }

    private fun prepareTestData(database: String, tableName: String, time: Long) {
        val writer = SimpleBitlapWriter(tableName, database)
        writer.use {
            it.write(
                listOf(
                    Event.of(time, Entity(1), Dimension("city" to "北京", "os" to "Mac"), Metric("vv", 1.0)),
                    Event.of(time, Entity(1), Dimension("city" to "北京", "os" to "Mac"), Metric("pv", 2.0)),
                    Event.of(time, Entity(1), Dimension("city" to "北京", "os" to "Windows"), Metric("vv", 1.0)),
                    Event.of(time, Entity(1), Dimension("city" to "北京", "os" to "Windows"), Metric("pv", 3.0)),
                    Event.of(time, Entity(2), Dimension("city" to "北京", "os" to "Mac"), Metric("vv", 1.0)),
                    Event.of(time, Entity(2), Dimension("city" to "北京", "os" to "Mac"), Metric("pv", 5.0)),
                )
            )
        }
    }
}

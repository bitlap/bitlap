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
        }

        "forbidden queries" {
            val table = randomString()
            sql("create table $table")
            shouldThrow<BitlapException> {
                sql("select *, a, b from $table") // star is forbidden
            }
            shouldThrow<BitlapException> {
                sql("select a, b from $table") // time filter is required
            }
            shouldThrow<BitlapException> {
                sql("select a, b from $table where _time=123") // one aggregation metric
            }
        }

        "single metric query" {
//            System.setProperty("calcite.debug", "true")
            val table = randomString()
            sql("create table $table")
            prepareTestData(table, 100L)
//            sql("select count(a) as a, count(distinct a) as a_dis, sum(b) as b from $table where _time=123").show()
//            sql("select count(a) as a, count(distinct a) as a_dis, sum(b) as b from $table where _time=123 and c='123'").show()
            sql("select bm_sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $table where _time=100 and c='123'").show()
//            sql("select _time, count(a) as a, count(distinct a) as a_dis, sum(b) as b from $table where _time=123 and c='123' group by _time").show()
//             sql("select 1+2*3, id, a from (select id, name as a from $table) t where id < 5 limit 100").show()
        }

        "simple query2" {
//            sql("create table test")
//            val sql = "select 1+2*3, id, a from (select id, name as a from test) t where id < 5 limit 100"
//    val sql = "select name, count(1), count(age) cnt from test where id < 5 and name = 'mimosa' group by name"
//            val sql = "select a, b, count(c) from test group by a, b"
//            sql(sql).show()
        }
    }

    private fun prepareTestData(tableName: String, time: Long) {
        val writer = SimpleBitlapWriter(tableName)
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

package org.bitlap.core.test.sql

import io.kotest.matchers.shouldBe
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

        "common database ddl statements" {
            sql("create table test")
            val sql = "select 1+2*3, id, a from (select id, name as a from test) t where id < 5 limit 100"
//    val sql = "select name, count(1), count(age) cnt from test where id < 5 and name = 'mimosa' group by name"
//            val sql = "select a, b, count(c) from test group by a, b"
            sql(sql).show()
        }
    }
}

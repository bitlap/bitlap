/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.test.sql

import org.bitlap.core.test.base.BaseLocalFsTest
import org.bitlap.core.test.base.SqlChecker

class ExplainTest : BaseLocalFsTest(), SqlChecker {

    init {
        "test explain sql" {
            val (db, table) = randomDBTable()
            sql("create table $db.$table")
            checkRows(
                "explain select count(vv) cvv, count(pv) cpv from $db.$table where _time = 100",
                listOf(
                    listOf(
                        "BitlapAggregate(group=[{}], cvv=[bm_count(\$0)], cpv=[bm_count(\$1)])\n" +
                            "  BitlapProject(vv=[\$1], pv=[\$0])\n" +
                            "    BitlapTableFilterScan(table=[[$db, $table]], class=[BitlapSqlQueryMetricTable], timeFilter=[[=(_time, 100)]], pruneFilter=[[]])\n"
                    )
                )
            )
        }
    }
}

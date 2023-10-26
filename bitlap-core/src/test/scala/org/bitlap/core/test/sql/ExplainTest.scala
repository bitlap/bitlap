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

class ExplainTest extends BaseLocalFsTest with SqlChecker {

  test("test explain sql") {
    val (db, table) = randomDBTable()
    sql(s"create table $db.$table")
    checkRows(
      s"explain select count(vv) cvv, count(pv) cpv from $db.$table where _time = 100",
      List(
        List(
          "BitlapAggregate(group=[{}], cvv=[bm_count($0)], cpv=[bm_count($1)])\n" +
            "  BitlapProject(vv=[$1], pv=[$0])\n" +
            s"    BitlapTableFilterScan(table=[[$db, $table]], class=[BitlapSqlQueryMetricTable], timeFilter=[[=(_time, 100)]], pruneFilter=[[]])\n"
        )
      )
    )
  }
}

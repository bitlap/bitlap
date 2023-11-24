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
package org.bitlap.testkit

import java.sql.*

import org.bitlap.jdbc.BitlapDataSource
import org.bitlap.network.{ ServerAddress, SyncConnection }
import org.bitlap.server.http.*

import org.scalatest.{ BeforeAndAfterAll, Inspectors }
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

import bitlap.rolls.core.jdbc.*

class JdbcOperationSpec extends BaseSpec {

  private val ds = new BitlapDataSource("jdbc:bitlap://127.0.0.1:23333/default")

  given Connection = ds.getConnection("root", "")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sql"create table if not exists $table"
    sql"load data 'classpath:simple_data.csv' overwrite table $table"
  }

  // Execute FakeDataUtilSpec to generate new mock data

  test("show database by jdbc") {
    val rs =
      sql"""
            show databases
       """
    val retx = ResultSetX[TypeRow1[String]](rs)
    val ret1 = retx.fetch()
    assert(ret1.nonEmpty)
    assert(ret1.head.columns[retx.Out]._1 == "default")
  }

  test("query by jdbc") {
    val rs = sql"""
       select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv
       from $table
       where _time >= 0
       group by _time
       """
    val ret1 = ResultSetX[TypeRow5[Long, Long, String, String, Int]](rs).fetch()
    assert(ret1.nonEmpty)
  }
}

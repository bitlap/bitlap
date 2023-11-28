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
import org.bitlap.server.http.model.underlying

import org.scalatest.{ BeforeAndAfterAll, Inspectors }
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

import bitlap.rolls.core.jdbc.*

class ClientOperationSpec extends BaseSpec {

  test("query by client") {
    val sync = new SyncConnection("root", "")

    sync.open(ServerAddress("127.0.0.1", 23333))

    val cr: List[List[(String, String)]] =
      sync.execute(s"create table if not exists $table").headOption.map(_.underlying).getOrElse(List.empty)

    val ld: List[List[(String, String)]] =
      sync
        .execute(s"load data 'classpath:simple_data.csv' overwrite table $table")
        .headOption
        .map(_.underlying)
        .getOrElse(List.empty)

    assert(cr == List(List(("Boolean", "true"))))
    assert(ld == List(List(("String", "true"))))

    val rs = sync
      .execute(
        s"""
          select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv
            from $table
            where _time >= 0
            group by _time
       """
      )
      .headOption
      .map(_.underlying)
      .getOrElse(List.empty)

    assert(rs.nonEmpty)
  }
}

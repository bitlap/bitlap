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

import org.bitlap.common.utils.DateEx
import org.bitlap.common.utils.DateEx.time
import org.bitlap.core.sql.udf.FunctionRegistry
import org.bitlap.core.test.base.BaseLocalFsTest
import org.bitlap.core.test.base.SqlChecker

class UdfTest extends BaseLocalFsTest with SqlChecker {

//  test("test lambda udf registry") {
//    // TODO (not support scala/kotlin lambda and inline class, should clean it like apache spark)
//    // for example, kotlin compiler generate class is: org.bitlap.core.test.sql.UdfTest$1$1
//    // FunctionRegistry.register("test_hello", (s: String) => s"hello $s")
//
//    // support common class and not inner class
//    FunctionRegistry.register("test_hello2", TestHello())
//    checkRows(
//      s"select test_hello2('a')",
//      List(List("hello a"))
//    )
//  }

  test("test hello udf") {
    checkRows(
      s"select hello(null), hello(1), hello('a')",
      List(List(null, "hello 1", "hello a"))
    )
  }

  test("test if udf") {
    checkRows(
      s"""
         |select
         |  if(true, 'a', 'b')
         | ,if(false, 'a', 'b')
         | ,if(1=1, 'a', 'b')
         | ,if(1=2, 'a', 'b')
         |""".stripMargin,
      List(List("a", "b", "a", "b"))
    )
  }

  test("test date_format udf") {
    checkRows(
      s"""
         |select
         |  date_format(1672502400000, 'yyyyMMdd') d1
         | ,date_format(1672502400000, 'yyyy-MM-dd HH:mm:ss') d2
         | ,date_format('2023-01-01', 'yyyyMMdd') d3
         | ,date_format('2023-01-01T00:00:00.000+08:00', 'yyyy-MM-dd HH:mm:ss') d3
         |""".stripMargin,
      // List(List("20230101", "2023-01-01 00:00:00", "20230101", "2023-01-01 00:00:00"))
      List(
        List(
          1672502400000L.time().toString("yyyyMMdd"),
          1672502400000L.time().toString("yyyy-MM-dd HH:mm:ss"),
          "2023-01-01".time().toString("yyyyMMdd"),
          "2023-01-01T00:00:00.000+08:00".time().toString("yyyy-MM-dd HH:mm:ss")
        )
      )
    )
  }
}

final class TestHello extends Function1[String, String] {
  override def apply(p1: String): String = s"hello $p1"
}

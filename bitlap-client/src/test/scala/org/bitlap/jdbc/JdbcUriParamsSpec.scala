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
package org.bitlap.jdbc

import scala.collection.immutable.ListMap

import org.scalatest.funsuite.AnyFunSuite

class JdbcUriParamsSpec extends AnyFunSuite:

  def shouldEquals[T, R](actual: => R, expect: => T): Unit =
    if !expect.equals(actual) then
      System.err.println(s"""
          |expect: $expect,
          |actual: $actual
          |""".stripMargin)
      assert(false)

  test("testUriParse") {
    val url1 = "jdbc:bitlap://host1:port1,host2:port2,host3:port3/;k1=v1?k2=v2"
    val url2 = "jdbc:bitlap://host1:port1,host2:port2,host3:port3/db;k1=v1?k2=v2"
    val url3 = "jdbc:bitlap://host1:port1,host2:port2,host3:port3?k2=v2"
    val url4 = "jdbc:bitlap://host1:port1,host2:port2,host3:port3"
    val url5 = "jdbc:bitlap://host1:port1,host2:port2,host3:port3?initFile=1sql;retries=3"

    val params1 = Utils.parseUri(url1)
    shouldEquals(params1.bitlapConfs, ListMap("k2" -> "v2"))
    shouldEquals(params1.sessionVars, ListMap("k1" -> "v1"))
    shouldEquals(params1.authorityList.toList, List("host1:port1", "host2:port2", "host3:port3"))
    shouldEquals(params1.dbName, "default")

    val params2 = Utils.parseUri(url2)
    shouldEquals(params2.bitlapConfs, ListMap("k2" -> "v2"))
    shouldEquals(params2.sessionVars, ListMap("k1" -> "v1"))
    shouldEquals(params2.authorityList.toList, List("host1:port1", "host2:port2", "host3:port3"))
    shouldEquals(params2.dbName, "db")

    val params3 = Utils.parseUri(url3)
    shouldEquals(params3.bitlapConfs, ListMap("k2" -> "v2"))
    shouldEquals(params3.sessionVars, ListMap())
    shouldEquals(params3.authorityList.toList, List("host1:port1", "host2:port2", "host3:port3"))
    shouldEquals(params3.dbName, "default")

    val params4 = Utils.parseUri(url4)
    shouldEquals(params4.bitlapConfs, ListMap())
    shouldEquals(params4.sessionVars, ListMap())
    shouldEquals(params4.authorityList.toList, List("host1:port1", "host2:port2", "host3:port3"))
    shouldEquals(params4.dbName, "default")

    val params5 = Utils.parseUri(url5)
    shouldEquals(params5.bitlapConfs, ListMap("initFile" -> "1sql", "retries" -> "3"))
  }

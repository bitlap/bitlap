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

import java.sql.DriverManager

import org.bitlap.Driver
import org.bitlap.common.exception.BitlapSQLException

class UtilsTest extends BaseClientTest {

  test("test Utils failure") {
    val url = "jdbc:xxxx://host1:port1,host2:port2,host3:port3?initFile=1sql;retries=3"
    assertThrows[BitlapSQLException] { Utils.parseUri(url) }
  }

  test("test Utils initSql") {
    val res = Utils.parseInitFile(this.getClass.getClassLoader.getResource("test.sql").getFile)
    assert(res.nonEmpty && res.headOption.contains("create table if not exists bitlap_test_table"))
  }

  test("test driver") {
    Class.forName(classOf[org.bitlap.Driver].getTypeName)
    val driver = DriverManager.getDriver("jdbc:bitlap://host1:port1")
    assert(driver.getMajorVersion == Constants.MAJOR_VERSION)
  }
}

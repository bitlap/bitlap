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
package org.bitlap.core.test

import org.bitlap.core.BitlapContext
import org.bitlap.core.mdm.BitlapEventWriter
import org.bitlap.core.test.base.BaseLocalFsTest
import org.bitlap.core.test.base.SqlChecker

class BitlapWriterTest extends BaseLocalFsTest with SqlChecker {

  test("test csv writer") {
    val (dbName, tableName) = randomDBTable()
    sql(s"create table $dbName.$tableName")
    val table  = BitlapContext.catalog.getTable(tableName, dbName)
    val writer = BitlapEventWriter(table, hadoopConf)
    val input  = classOf[BitlapWriterTest].getClassLoader.getResourceAsStream("simple_data.csv")
    writer.writeCsv(input)
    checkRows(
      s"select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $table where _time >= 100 group by _time",
      List(List(100, 4, 12, 3), List(200, 4, 12, 3))
    )
    checkRows(
      s"select sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv from $table where _time >= 100",
      List(List(8, 24, 3))
    )
  }
}

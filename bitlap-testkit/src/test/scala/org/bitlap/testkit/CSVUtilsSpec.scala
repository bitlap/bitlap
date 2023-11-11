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

import java.io.File

import org.bitlap.network.enumeration.{ OperationType, TypeId }
import org.bitlap.network.handles.OperationHandle
import org.bitlap.network.serde.BitlapSerde

import org.scalatest.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

/** csv test
 */
class CSVUtilsSpec extends AnyFunSuite with CSVUtils with should.Matchers {

  test("test CSVUtils") {
    val csv = readClasspathCSVData("simple_data.csv")
    println(csv.headOption)
    val tmp = new File("./simple_data.csv")
    val ret = writeCSVData(tmp, csv)
    assert(ret)
    tmp.delete()
    csv.headOption.toString shouldEqual "Some(Metric(1666195200,14,List(Dimension(os,Windows 8), Dimension(city,西安)),pv,712739626))"
  }

  test("test MockAsyncProtocol") {
    val asyncProtocol = new MockAsync()
    val ret           = asyncProtocol.fetchResults(OperationHandle(OperationType.ExecuteStatement), 50, 1)
    val syncRet = zio.Unsafe.unsafe { implicit rt =>
      zio.Runtime.default.unsafe.run(ret).getOrThrowFiberFailure()
    }
    println(syncRet)
    val head = syncRet.results.rows.head.values
    val value = List(
      BitlapSerde.deserialize[Long](TypeId.LongType, head(0)),
      BitlapSerde.deserialize[Int](TypeId.IntType, head(1)),
      BitlapSerde.deserialize[String](TypeId.StringType, head(2)),
      BitlapSerde.deserialize[String](TypeId.StringType, head(3)),
      BitlapSerde.deserialize[Long](TypeId.LongType, head(4))
    )
    value shouldEqual List(1666195200, 14, "Windows 8", "pv", 712739626)
  }
}

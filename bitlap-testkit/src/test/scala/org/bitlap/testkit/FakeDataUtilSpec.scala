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

import java.io.*

import org.scalatest.funsuite.AnyFunSuite

class FakeDataUtilSpec extends AnyFunSuite with CSVUtils {

  test("testFakeData") {
    val s: List[Metric] = FakeDataUtils.randMetrics(50)
    val file            = new File("simple_data.csv")
    writeCSVData(file, s)
    assert(checkCSVData(file)(ms => ms == s))
    file.delete()
  }

}

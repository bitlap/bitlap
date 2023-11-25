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

import org.bitlap.jdbc.BitlapDataSource

import org.scalatest.{ BeforeAndAfterAll, Inspectors }
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

abstract class BaseSpec extends AnyFunSuite with BeforeAndAfterAll with should.Matchers with Inspectors with CSVUtils {
  lazy val table = s"test_table_${FakeDataUtils.randEntityNumber}"

  // TODO
  val server = new Thread {
    override def run(): Unit = MockBitlapServer.main(scala.Array.empty)
  }

  override protected def beforeAll(): Unit = {
    server.setDaemon(true)
    server.start()
    Thread.sleep(5000L)
  }
}

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
package org.bitlap.common.test

import scala.concurrent.duration._

import org.bitlap.common.BitlapConf
import org.bitlap.common.conf.BitlapConfKeys

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

class BitlapConfTest extends AnyFunSuite with should.Matchers {

  test("test simple bitlap configuration") {
    val conf = BitlapConf()
    conf.get(BitlapConfKeys.PROJECT_NAME) shouldBe "bitlap"
    conf.get(BitlapConfKeys.ROOT_DIR) shouldBe "/usr/local/var/bitlap"
    conf.get(BitlapConfKeys.LOCAL_DIR) shouldBe "/usr/local/var/bitlap"
    conf.get(BitlapConfKeys.NODE_HOST) shouldBe "127.0.0.1:23333"
    conf.get(BitlapConfKeys.NODE_RAFT_HOST) shouldBe "127.0.0.1:24333"
    conf.get(BitlapConfKeys.NODE_RAFT_PEERS) shouldBe "127.0.0.1:24333"
    conf.get(BitlapConfKeys.NODE_RAFT_TIMEOUT) shouldBe 5.seconds
  }

  test("test bitlap configuration with system properties and OS environment variables") {
    System.setProperty(BitlapConfKeys.NODE_HOST.getSysKey, "127.0.0.1:13333")
    withEnvironment(BitlapConfKeys.NODE_RAFT_HOST.getEnvKey, "127.0.0.1:14333") {
      val conf = BitlapConf()
      conf.get(BitlapConfKeys.NODE_HOST) shouldBe "127.0.0.1:13333"
      conf.get(BitlapConfKeys.NODE_RAFT_HOST) shouldBe "127.0.0.1:14333"
    }
  }

  test("test bitlap configuration with parameters") {
    val conf = BitlapConf(Map(BitlapConfKeys.NODE_RAFT_TIMEOUT.key -> "10s"))
    conf.get(BitlapConfKeys.NODE_RAFT_TIMEOUT) shouldBe 10.seconds
  }
}

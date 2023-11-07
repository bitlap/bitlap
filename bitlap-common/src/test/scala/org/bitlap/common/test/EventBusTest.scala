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

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.parallel.CollectionConverters.*

import org.bitlap.common.BitlapEvent
import org.bitlap.common.EventBus
import org.bitlap.common.extension.*

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

class TestEvent1              extends BitlapEvent
case class TestEvent2(i: Int) extends BitlapEvent

class EventBusTest extends AnyFunSuite with should.Matchers {

  test("Test simple eventbus") {
    val count    = AtomicInteger(0)
    val eventBus = EventBus().also { _.start() }
    eventBus
      .subscribe[TestEvent1] { _ =>
        count.incrementAndGet()
      }
      .subscribe[TestEvent1] { _ =>
        count.addAndGet(2)
      }
      .subscribe[TestEvent2] { it =>
        count.addAndGet(it.i)
      }

    (0 until 1000).par.foreach { i =>
      eventBus
        .post(TestEvent1())
        .post(TestEvent2(i + 1))
    }

    Thread.sleep(1000L)
    count.get() shouldBe (1000 + 1000 * 2 + 500500)
    eventBus.isStarted shouldBe true
    eventBus.isShutdown shouldBe false
    eventBus.close()
    eventBus.isStarted shouldBe false
    eventBus.isShutdown shouldBe true
  }
}

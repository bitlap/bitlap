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

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.bitlap.common.BitlapEvent
import org.bitlap.common.EventBus
import java.util.concurrent.atomic.AtomicInteger

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/7/21
 */

class TestEvent1 : BitlapEvent
data class TestEvent2(val i: Int) : BitlapEvent

class EventBusTest : StringSpec({

    "Test simple eventbus" {
        val count = AtomicInteger(0)
        val eventBus = EventBus().apply { start() }
        eventBus.subscribe<TestEvent1> {
            count.incrementAndGet()
        }.subscribe<TestEvent1> {
            count.addAndGet(2)
        }.subscribe<TestEvent2> {
            count.addAndGet(it.i)
        }

        coroutineScope {
            repeat(1000) {
                launch {
                    eventBus
                        .post(TestEvent1())
                        .post(TestEvent2(it + 1)) // start from zero
                }
            }
        }

        delay(100)
        count.get() shouldBe (1000 + 1000 * 2 + 500500)
        eventBus.isStarted() shouldBe true
        eventBus.isShutdown() shouldBe false
        eventBus.close()
        eventBus.isStarted() shouldBe false
        eventBus.isShutdown() shouldBe true
    }
})

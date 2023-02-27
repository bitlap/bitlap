/* Copyright (c) 2023 bitlap.org */
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

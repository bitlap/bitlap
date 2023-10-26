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
package org.bitlap.common

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Event Bus system.
 */

interface BitlapEvent
typealias BitlapSubscriber<E> = (e: E) -> Unit

open class EventBus(
    val executor: ExecutorService = Executors.newWorkStealingPool()
) : LifeCycleWrapper() {
    val subscribers = ConcurrentHashMap<Class<out BitlapEvent>, MutableList<BitlapSubscriber<*>>>()

    inline fun <reified E : BitlapEvent> subscribe(noinline subscriber: BitlapSubscriber<E>): EventBus {
        val subscribers = this.subscribers.computeIfAbsent(E::class.java) { mutableListOf() }
        subscribers.add(subscriber as BitlapSubscriber<*>)
        return this
    }

    @Suppress("UNCHECKED_CAST")
    inline fun <reified E : BitlapEvent> postK(e: E): EventBus {
        this.subscribers[E::class.java]?.forEach {
            executor.execute {
                (it as BitlapSubscriber<E>).invoke(e)
            }
        }
        return this
    }

    fun <E : BitlapEvent> post(e: E): EventBus {
        this.subscribers[e.javaClass]?.forEach {
            executor.execute {
                (it as BitlapSubscriber<E>).invoke(e)
            }
        }
        return this
    }

    @Synchronized
    override fun start() {
        super.start()
        log.info("EventBus system has been started.")
    }

    @Synchronized
    override fun close() {
        super.close()
        if (this.executor.isShutdown) {
            return
        }
        runCatching {
            this.executor.shutdown()
            if (!this.executor.awaitTermination(2, TimeUnit.SECONDS)) {
                this.executor.shutdownNow()
            }
        }.onFailure {
            log.error("Error when closing EventBus, cause: ", it)
        }
        log.info("EventBus system has been closed.")
    }
}

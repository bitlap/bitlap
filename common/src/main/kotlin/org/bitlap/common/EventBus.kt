package org.bitlap.common

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Desc: Event Bus system.
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
    inline fun <reified E : BitlapEvent> post(e: E): EventBus {
        this.subscribers[E::class.java]?.forEach {
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

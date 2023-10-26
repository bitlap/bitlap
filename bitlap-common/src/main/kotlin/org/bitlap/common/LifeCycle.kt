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

import java.io.Closeable

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/4/22
 */
interface LifeCycle : Closeable {
    fun start()
    fun isStarted(): Boolean
    fun isShutdown(): Boolean
}

abstract class LifeCycleWrapper : LifeCycle {
    @JvmField
    protected val log = logger(this::class.java)

    @Volatile
    @JvmField
    protected var started = false

    @Volatile
    @JvmField
    protected var shutdown = true

    override fun start() {
        shutdown = false
        started = true
    }

    override fun close() {
        started = false
        shutdown = true
    }

    override fun isStarted(): Boolean = this.started
    override fun isShutdown(): Boolean = this.shutdown
}

abstract class LifeCycleThread(val name: String, val daemon: Boolean) : LifeCycleWrapper(), Runnable {

    constructor(name: String) : this(name, false)

    val thread: Thread by lazy {
        Thread(this, name).also {
            it.isDaemon = daemon
            it.setUncaughtExceptionHandler { t, e -> this.handleException(t, e) }
        }
    }

    protected fun handleException(t: Thread, e: Throwable) {
        log.error(e) { "Exception occurred from thread ${t.name}" }
    }

    @Synchronized
    override fun start() {
        if (this.isStarted()) {
            log.info { "$name has been started." }
            return
        }
        thread.start()
        super.start()
    }

    override fun close() {
        if (this.isShutdown()) {
            log.info { "$name has been closed." }
            return
        }
        runCatching {
            if (!thread.isInterrupted) {
                log.info { "Starting to close thread: $name" }
                thread.interrupt()
            }
        }.onFailure {
            log.warn(it) { "Error when closing thread: $name" }
        }
        super.close()
    }

    fun join() {
        runCatching { this.thread.join() }
    }
}

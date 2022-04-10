/* Copyright (c) 2022 bitlap.org */
package org.bitlap.common

import cn.hutool.core.thread.ThreadUtil
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
        ThreadUtil.newThread(this, name, daemon).also {
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
        ThreadUtil.waitForDie(this.thread)
    }
}

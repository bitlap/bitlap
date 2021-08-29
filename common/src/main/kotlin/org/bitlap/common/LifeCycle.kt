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

    @Volatile
    protected var started = false
    @Volatile
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

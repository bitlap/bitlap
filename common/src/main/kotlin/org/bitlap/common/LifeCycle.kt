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

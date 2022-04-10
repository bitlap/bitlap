/* Copyright (c) 2022 bitlap.org */
package org.bitlap.common

import cn.hutool.core.date.DateUtil
import mu.KLogger
import mu.KotlinLogging
import java.time.Duration

/**
 * Do [func] when [flag] is true,
 * if flag is false, return [t] only
 */
fun <T> doIf(flag: Boolean, t: T, func: (T) -> T): T {
    if (flag) {
        return func.invoke(t)
    }
    return t
}

/**
 * see [kotlin.system.measureTimeMillis] and [kotlin.time.measureTimedValue]
 */
fun <T> elapsed(block: () -> T): Pair<T, Duration> {
    val timer = DateUtil.timer()
    return block() to Duration.ofMillis(timer.interval())
}

/**
 * Create kotlin logger wrapper
 */
fun logger(func: () -> Unit): KLogger {
    return KotlinLogging.logger(func)
}

fun logger(clazz: Class<*>): KLogger {
    return logger(clazz.name)
}

fun logger(name: String): KLogger {
    return KotlinLogging.logger(name)
}

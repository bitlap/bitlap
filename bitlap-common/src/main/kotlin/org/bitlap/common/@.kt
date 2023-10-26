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
    val start = System.currentTimeMillis()
    return block() to Duration.ofMillis(System.currentTimeMillis() - start)
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

/**
 * like sql nvl
 */
fun <T> nvl(t: T?, default: T?): T? {
    return t ?: default
}

/**
 * like sql coalesce
 */
fun <T> coalesce(vararg t: T?): T? {
    return t.find { it != null }
}

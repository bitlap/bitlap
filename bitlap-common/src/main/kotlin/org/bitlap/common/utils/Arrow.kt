/* Copyright (c) 2022 bitlap.org */
package org.bitlap.common.utils

import arrow.core.None
import arrow.core.Option
import arrow.core.Some

/**
 * common Arrow utils
 */
object Arrow {

    /**
     * Add an option value to list
     */
    operator fun <T> List<T>.plus(o: Option<T>): List<T> {
        return when (o) {
            is None -> this
            is Some -> this + o.value
        }
    }
}

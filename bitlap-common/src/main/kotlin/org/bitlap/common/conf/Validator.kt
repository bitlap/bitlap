/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.common.conf

/**
 * Validator for [[BitlapConfKey]] value.
 */
fun interface Validator<T> {

    /**
     * check [t] if is valid.
     */
    fun validate(t: T?): Boolean
}

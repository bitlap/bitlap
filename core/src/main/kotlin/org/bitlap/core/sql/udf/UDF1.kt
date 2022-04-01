/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.sql.udf

/**
 * A generic interface for defining user-defined functions.
 *
 * [V]: input value type
 * [R]: result type
 */
interface UDF1<V, R> : UDF {

    /**
     * eval with one input.
     */
    fun eval(input: V): R
}

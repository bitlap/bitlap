/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.udf

/**
 * A generic interface for defining user-defined functions.
 *
 * [V1]: input value type
 * [V2]: input value type
 * [R]: result type
 */
interface UDF2<V1, V2, R> : UDF {

    /**
     * eval with two inputs.
     */
    fun eval(input1: V1, input2: V2): R
}

package org.bitlap.core.sql.udf

/**
 * A generic interface for defining user-defined functions.
 *
 * [V1]: input value type
 * [V2]: input value type
 * [V3]: input value type
 * [R]: result type
 */
interface UDF3<V1, V2, V3, R> : UDF {

    /**
     * eval with three inputs.
     */
    fun eval(input1: V1, input2: V2, input3: V3): R
}

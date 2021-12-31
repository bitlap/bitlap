package org.bitlap.core.sql.udf

/**
 * A generic interface for defining user-defined functions.
 *
 * [R]: result type
 */
interface UDF0<R> : UDF {

    /**
     * eval with no inputs.
     */
    fun eval(): R
}

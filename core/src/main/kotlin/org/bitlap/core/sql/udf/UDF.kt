package org.bitlap.core.sql.udf

/**
 * A generic interface for defining user-defined functions.
 *
 * [V]: input value type
 * [R]: result type
 */
interface UDF<V, R> {

    /**
     * eval with one input.
     * for more inputs, see [org.apache.calcite.schema.impl.ScalarFunctionImpl.create]
     */
    fun eval(input: V): R
}

/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.sql.udf

import org.apache.calcite.sql.type.SqlReturnTypeInference
import org.apache.calcite.sql.type.SqlTypeName

/**
 * A generic interface for defining user-defined aggregate functions.
 *
 * [A]: accumulator type
 * [V]: input value type
 * [R]: result type
 */
interface UDAF<A, V, R> {

    /**
     * agg function name
     */
    val name: String

    /**
     * input types
     */
    val inputTypes: List<SqlTypeName>

    /**
     * agg result type
     */
    val resultType: SqlReturnTypeInference

    /**
     * agg init value
     */
    fun init(): A

    /**
     * add one input to accumulator
     */
    fun add(accumulator: A, input: V): A

    /**
     * merge two accumulator
     */
    fun merge(accumulator1: A, accumulator2: A): A

    /**
     * agg result
     */
    fun result(accumulator: A): R
}

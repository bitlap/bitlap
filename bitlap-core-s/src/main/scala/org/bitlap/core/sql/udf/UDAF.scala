/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.udf

import org.apache.calcite.sql.`type`.SqlReturnTypeInference
import org.apache.calcite.sql.`type`.SqlTypeName

/**
 * A generic interface for defining user-defined aggregate functions.
 *
 * [A]: accumulator type
 * [V]: input value type
 * [R]: result type
 */
trait UDAF[A, V, R] extends UDF {

    /**
     * agg function name
     */
    override val name: String

    /**
     * input types
     */
    override val inputTypes: List[SqlTypeName]

    /**
     * agg result type
     */
    override val resultType: SqlReturnTypeInference

    /**
     * agg init value
     */
    def init(): A

    /**
     * add one input to accumulator
     */
    def add(accumulator: A, input: V): A

    /**
     * merge two accumulator
     */
    def merge(accumulator1: A, accumulator2: A): A

    /**
     * agg result
     */
    def result(accumulator: A): R
}

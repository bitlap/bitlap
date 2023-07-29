/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.udf

import org.apache.calcite.sql.type.SqlReturnTypeInference
import org.apache.calcite.sql.type.SqlTypeName

/**
 * A generic interface for defining user-defined functions.
 *
 * for more inputs, see [org.apache.calcite.schema.impl.ScalarFunctionImpl.create]
 */
interface UDF {

    /**
     * function name
     */
    val name: String

    /**
     * input types
     */
    val inputTypes: List<SqlTypeName>

    /**
     * result type
     */
    val resultType: SqlReturnTypeInference
}

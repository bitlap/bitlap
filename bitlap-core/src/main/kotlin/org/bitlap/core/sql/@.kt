/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.sql

import org.apache.calcite.sql.type.ReturnTypes
import org.apache.calcite.sql.type.SqlReturnTypeInference
import org.apache.calcite.sql.type.SqlTypeName

/**
 * infer sql type from [SqlTypeName]
 */
fun SqlTypeName.infer(): SqlReturnTypeInference {
    return ReturnTypes.explicit(this)
}

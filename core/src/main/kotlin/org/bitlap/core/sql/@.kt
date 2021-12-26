package org.bitlap.core.sql

import org.apache.calcite.sql.type.ReturnTypes
import org.apache.calcite.sql.type.SqlReturnTypeInference
import org.apache.calcite.sql.type.SqlTypeName

/**
 * alias
 */
typealias TimeFilterFun = (Long) -> Boolean

/**
 * infer sql type from [SqlTypeName]
 */
fun SqlTypeName.infer(): SqlReturnTypeInference {
    return ReturnTypes.explicit(this)
}

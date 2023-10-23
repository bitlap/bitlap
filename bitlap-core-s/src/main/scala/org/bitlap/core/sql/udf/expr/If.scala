/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.udf.expr

import org.apache.calcite.sql.`type`.ReturnTypes
import org.apache.calcite.sql.`type`.SqlReturnTypeInference
import org.apache.calcite.sql.`type`.SqlTypeName
import org.bitlap.core.sql.udf.UDF3
import org.bitlap.core.sql.udf.UDFNames

/**
 * IF UDF
 *
 * expression: if(condition, expr1, expr2)
 * return: if condition is true then $expr1, else $expr2
 */
class If extends UDF3[Boolean, Any, Any, Any] {

    override val name: String = UDFNames.`if`
    override val inputTypes: List[SqlTypeName] = List(SqlTypeName.BOOLEAN, SqlTypeName.ANY, SqlTypeName.ANY)
    override val resultType: SqlReturnTypeInference = ReturnTypes.ARG1_NULLABLE

    override def eval(input1: Boolean, input2: Any, input3: Any): Any = {
        if (input1) {
            return input2
        }
        return input3
    }
}

/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.sql.udf

import org.apache.calcite.sql.type.ReturnTypes
import org.apache.calcite.sql.type.SqlReturnTypeInference
import org.apache.calcite.sql.type.SqlTypeName

/**
 * IF UDF
 *
 * expression: if(condition, expr1, expr2)
 * return: if condition is true then $expr1, else $expr2
 */
class UdfIf : UDF3<Boolean, Any?, Any?, Any?> {
    companion object {
        const val NAME = UdfNames.`if`
    }

    override val name: String = NAME
    override val inputTypes: List<SqlTypeName> = listOf(SqlTypeName.BOOLEAN, SqlTypeName.ANY, SqlTypeName.ANY)
    override val resultType: SqlReturnTypeInference = ReturnTypes.ARG1_NULLABLE

    override fun eval(input1: Boolean, input2: Any?, input3: Any?): Any? {
        if (input1) {
            return input2
        }
        return input3
    }
}

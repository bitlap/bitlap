/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.sql.udf

import org.apache.calcite.sql.type.SqlReturnTypeInference
import org.apache.calcite.sql.type.SqlTypeName
import org.bitlap.core.sql.infer

/**
 * Hello UDF
 *
 * expression: hello(expr)
 * return: hello $expr
 */
class UdfHello : UDF1<Any?, String?> {
    companion object {
        const val NAME = UdfNames.hello
    }

    override val name: String = NAME
    override val inputTypes: List<SqlTypeName> = listOf(SqlTypeName.ANY)
    override val resultType: SqlReturnTypeInference = SqlTypeName.VARCHAR.infer()

    override fun eval(input: Any?): String? {
        if (input == null) {
            return null
        }
        return "hello $input"
    }
}

/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.udf.date

import org.apache.calcite.sql.type.SqlReturnTypeInference
import org.apache.calcite.sql.type.SqlTypeName
import org.bitlap.core.sql.infer
import org.bitlap.core.sql.udf.UDF2
import org.bitlap.core.sql.udf.UDFNames
import org.joda.time.DateTime

/**
 * date_format UDF
 *
 * expression: date_format(time, pattern)
 */
class DateFormat : UDF2<Any?, String, String?> {

    override val name: String = UDFNames.date_format
    override val inputTypes: List<SqlTypeName> = listOf(SqlTypeName.ANY, SqlTypeName.VARCHAR)
    override val resultType: SqlReturnTypeInference = SqlTypeName.VARCHAR.infer()

    override fun eval(input1: Any?, input2: String): String? {
        return when (input1) {
            null -> null
            else -> DateTime(input1).toString(input2)
        }
    }
}

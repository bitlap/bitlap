/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.sql.udf

import org.apache.calcite.sql.type.SqlReturnTypeInference
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.commons.lang3.time.FastDateFormat
import org.bitlap.core.sql.infer
import java.util.Locale
import java.util.TimeZone

/**
 * date_format UDF
 *
 *  expression: date_format(_time)
 * return: yyyy-MM-dd
 */
class UdfDateFormat : UDF1<Long, String> {
    companion object {
        const val NAME = "date_format"
    }

    override val name: String = NAME
    override val inputTypes: List<SqlTypeName> = listOf(SqlTypeName.BIGINT)
    override val resultType: SqlReturnTypeInference = SqlTypeName.VARCHAR.infer()

    override fun eval(input: Long): String {
        val zoneId = TimeZone.getTimeZone("GMT+8")
        val sdf = FastDateFormat.getInstance("yyyy-MM-dd", zoneId, Locale.CHINA)
        return sdf.format(input)
    }
}

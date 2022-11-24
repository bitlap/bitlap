/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.sql.udf

import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.sql.SqlBasicCall
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlSelect
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
class UdfDateFormat : UDF1<Long, String>, UdfValidator {
    companion object {
        const val NAME = UdfNames.date_format
    }

    override val name: String = NAME
    override val inputTypes: List<SqlTypeName> = listOf(SqlTypeName.BIGINT)
    override val resultType: SqlReturnTypeInference = SqlTypeName.VARCHAR.infer()

    override fun eval(input: Long): String {
        val zoneId = TimeZone.getTimeZone("GMT+8")
        val sdf = FastDateFormat.getInstance("yyyy-MM-dd", zoneId, Locale.CHINA)
        return sdf.format(input)
    }

    // TODO make parameter more generic
    override fun validate(select: SqlSelect, targetRowType: RelDataType): Boolean {
        val checkSelectGroup = select.selectList.map {
            when (it) {
                is SqlBasicCall -> {
                    val op = it.operator.name == UdfNames.date_format
                    select.group == null || !op || (
                        select.group!!.any { gp ->
                            when (gp) {
                                is SqlBasicCall -> if (gp.operator.name == it.operator.name) {
                                    val selectOpNames = it.operandList.flatMap { gpit ->
                                        when (gpit) {
                                            is SqlIdentifier -> gpit.names
                                            else -> emptyList<String>()
                                        }
                                    }

                                    val groupOpNames = gp.operandList.flatMap { gpit ->
                                        when (gpit) {
                                            is SqlIdentifier -> gpit.names
                                            else -> emptyList<String>()
                                        }
                                    }
                                    groupOpNames.containsAll(selectOpNames)
                                } else true
                                else -> false
                            }
                        }
                        )
                }
                else -> true
            }
        }
        return checkSelectGroup.all { it }
    }
}

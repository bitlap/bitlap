/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.sql.udf

import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.sql.SqlSelect

/**
 *
 * @author 梦境迷离
 * @version 1.0,2022/11/24
 */
interface UdfValidator {

    fun validate(select: SqlSelect, targetRowType: RelDataType)
}

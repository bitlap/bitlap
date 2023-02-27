/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.sql.table

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.linq4j.Linq4j
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.schema.ScannableTable
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql.type.SqlTypeName

/**
 * Desc: table for ddl operation
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/9/4
 */
open class BitlapSqlDdlTable(
    open val rowTypes: List<Pair<String, SqlTypeName>>,
    open val operator: (DataContext) -> List<Array<Any?>>
) : AbstractTable(), ScannableTable {

    override fun getRowType(typeFactory: RelDataTypeFactory): RelDataType {
        return typeFactory.builder().let {
            this.rowTypes.forEach { rowType -> it.add(rowType.first, rowType.second) }
            it.build()
        }
    }

    override fun scan(root: DataContext): Enumerable<Array<Any?>> {
        return Linq4j.asEnumerable(this.operator.invoke(root))
    }
}

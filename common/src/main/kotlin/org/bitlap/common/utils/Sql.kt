package org.bitlap.common.utils

import org.bitlap.common.utils.internal.DBTable
import org.bitlap.common.utils.internal.DBTablePrinter
import java.sql.ResultSet

/**
 * common sql utils
 */
object Sql {

    /**
     * print sql result beautifully
     */
    fun ResultSet.toTable(): DBTable {
        return DBTablePrinter.getDBTable(this)
    }
}

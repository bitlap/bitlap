package org.bitlap.common.utils

import org.bitlap.common.utils.internal.DBTablePrinter
import java.sql.ResultSet

/**
 * common sql utils
 */
object Sql {

    /**
     * print sql result beautifully
     */
    fun ResultSet.show() {
        DBTablePrinter.printResultSet(this)
    }
}

/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.common.utils

import org.bitlap.common.utils.internal.DBTable
import org.bitlap.common.utils.internal.DBTablePrinter
import java.sql.ResultSet

/**
 * common sql utils
 */
object SqlEx {

    /**
     * print sql result beautifully
     */
    @JvmStatic
    fun ResultSet.toTable(): DBTable {
        return DBTablePrinter.from(this)
    }
}

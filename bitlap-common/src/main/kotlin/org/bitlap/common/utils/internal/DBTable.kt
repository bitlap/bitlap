/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.common.utils.internal

/**
 * row wrapper
 */
data class DBTable(
    val tableNames: List<String>,
    val rowCount: Int,
    val columns: List<DBTablePrinter.Column>,
) {

    fun show() {
        DBTablePrinter.print(this)
    }
}

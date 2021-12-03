package org.bitlap.core.test.base

import org.bitlap.common.utils.Sql.toTable
import org.bitlap.common.utils.internal.DBTable
import org.bitlap.core.sql.QueryExecution

/**
 * Some sql check utils
 */
interface SqlChecker {

    /**
     * execute sql statement
     */
    fun sql(statement: String): SqlResult {
        val rs = QueryExecution(statement).execute()
        return SqlResult(statement, rs.toTable())
    }
}

class SqlResult(private val statement: String, private val table: DBTable) {

    internal val result: List<List<Any?>> by lazy {
        mutableListOf<List<Any?>>().apply {
            (0 until table.rowCount).forEach { r ->
                add(table.columns.map { it.getTypeValue(r) })
            }
        }
    }

    val size = this.result.size

    fun show(): SqlResult {
        table.show()
        return this
    }

    override fun toString(): String {
        return this.result.toString()
    }

    override fun hashCode(): Int {
        return this.result.hashCode()
    }

    override fun equals(other: Any?): Boolean {
        return this.result == other
    }
}

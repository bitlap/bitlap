package org.bitlap.core.test.base

import org.bitlap.common.utils.Sql.show
import org.bitlap.core.sql.QueryExecution

/**
 * Some sql check utils
 */
interface SqlChecker {

    /**
     * execute sql statement
     */
    fun sql(statement: String): SqlResult {
        return SqlResult(statement)
    }
}

class SqlResult(private val statement: String) {

    private val result: List<List<Any?>> by lazy {
        mutableListOf<List<Any?>>().apply {
            val rs = QueryExecution(statement).execute()
            val colSize = rs.metaData.columnCount
            while (rs.next()) {
                add(
                    (1..colSize).map {
                        rs.getObject(it)
                    }
                )
            }
        }
    }

    val size = this.result.size

    fun show(): SqlResult {
        QueryExecution(statement).execute().show()
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

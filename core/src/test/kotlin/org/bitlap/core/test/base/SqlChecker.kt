package org.bitlap.core.test.base

import io.kotest.assertions.Actual
import io.kotest.assertions.Expected
import io.kotest.assertions.failure
import io.kotest.matchers.collections.printed
import io.kotest.matchers.shouldBe
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

    /**
     * check rows
     */
    fun checkRows(statement: String, rows: List<List<Any?>>) {
        val result = sql(statement).result
        try {
            result.size shouldBe rows.size
            result.zip(rows).forEach { (r1, r2) ->
                r1.size shouldBe r2.size
                r1.zip(r2).forEach { (v1, v2) ->
                    v1 shouldBe v2
                }
            }
        } catch (e: Throwable) {
            when (e) {
                is AssertionError ->
                    throw failure(Expected(rows.printed()), Actual(result.printed()))
                else ->
                    throw e
            }
        }
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

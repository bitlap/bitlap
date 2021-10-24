package org.bitlap.core.sql

import org.apache.calcite.tools.RelRunners
import org.bitlap.common.exception.BitlapException
import org.bitlap.core.BitlapContext
import java.sql.ResultSet

/**
 * Desc: Execution for each query
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/8/6
 */
class QueryExecution(private val statement: String) {

    private val planner = BitlapContext.sqlPlanner

    fun execute(): ResultSet {
        try {
            return QueryContext.use {
                it.statement = statement
                val plan = planner.parse(statement)
                RelRunners.run(plan).executeQuery()
            }
        } catch (e: Exception) {
            when (e.cause) {
                is BitlapException -> throw e.cause!!
                else -> throw BitlapException(statement, e)
            }
        }
    }
}

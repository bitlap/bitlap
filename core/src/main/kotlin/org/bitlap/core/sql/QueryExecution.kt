package org.bitlap.core.sql

import org.apache.calcite.tools.RelRunners
import org.bitlap.common.BitlapConf
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

    private val runtimeConf: BitlapConf = BitlapContext.bitlapConf // TODO: merge session conf
    private val planner = BitlapContext.sqlPlanner

    fun execute(): ResultSet {
        try {
            return QueryContext.use {
                it.runtimeConf = runtimeConf
                it.statement = statement
                val plan = planner.parse(statement).relOpt
                RelRunners.run(plan).executeQuery()
            }
        } catch (e: Throwable) {
            when (e) {
                is BitlapException -> throw e
                else -> throw BitlapException(statement, e)
            }
        }
    }
}

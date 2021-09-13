package org.bitlap.core.sql

import org.apache.calcite.tools.RelRunners
import org.bitlap.core.BitlapContext
import java.sql.ResultSet

/**
 * Desc:
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/8/6
 */
class QueryExecution(private val statement: String) {

    private val planner = BitlapContext.sqlPlanner

    fun execute(): ResultSet {
        val plan = planner.parse(statement)
        return RelRunners.run(plan).executeQuery()
    }
}

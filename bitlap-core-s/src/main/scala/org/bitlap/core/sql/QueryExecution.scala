/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql

import org.apache.calcite.tools.RelRunners
import org.bitlap.common.BitlapConf
import org.bitlap.common.exception.BitlapException
import org.bitlap.core.BitlapContext
import org.bitlap.core.QueryResult
import org.bitlap.core.sql.parser.ddl.SqlUseDatabase

import scala.jdk.CollectionConverters._

/**
 * Desc: Execution for each query
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/8/6
 */
class QueryExecution(
    private val statement: String,
    private val currentSchema: String
) {

    private val runtimeConf: BitlapConf = BitlapContext.bitlapConf // TODO (merge session conf)
    private val planner = BitlapContext.sqlPlanner

    def execute(): QueryResult = {
        try {
            return QueryContext.use { ctx =>
                ctx.runtimeConf = runtimeConf
                ctx.currentSchema = currentSchema
                ctx.statement = statement
                this.execute0(statement)
            }
        } catch {
          case e: BitlapException => throw e
          case e => throw BitlapException(statement, Map.empty[String, String].asJava, e)
        }
    }

    private def execute0(statement: String): QueryResult = {
        var useSchema = currentSchema
        val plan = planner.parse(statement)
        val result = RelRunners.run(plan.relOpt).executeQuery()
        // some special operations
        plan.sqlNode match
          case node: SqlUseDatabase =>
            useSchema = node.useDatabase
          case _ =>

        return QueryResult(result, useSchema)
    }
}

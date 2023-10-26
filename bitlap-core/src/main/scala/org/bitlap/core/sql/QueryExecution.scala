/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql

import scala.jdk.CollectionConverters._

import org.bitlap.common.BitlapConf
import org.bitlap.common.exception.BitlapException
import org.bitlap.core.*
import org.bitlap.core.BitlapContext
import org.bitlap.core.sql.parser.ddl.SqlUseDatabase

import org.apache.calcite.tools.RelRunners

/** Execution for each query
 */
class QueryExecution(
  private val statement: String,
  private val currentSchema: String) {

  private val runtimeConf: BitlapConf = BitlapContext.bitlapConf // TODO (merge session conf)
  private val planner                 = BitlapContext.sqlPlanner

  def execute(): QueryResult = {
    try {
      QueryContext.use { ctx =>
        ctx.runtimeConf = runtimeConf
        ctx.currentSchema = currentSchema
        ctx.statement = statement
        this.execute0(statement)
      }
    } catch {
      case e: BitlapException => throw e
      case e                  => throw new BitlapException(statement, Map.empty[String, String].asJava, e)
    }
  }

  private def execute0(statement: String): QueryResult = {
    var useSchema = currentSchema
    val plan      = planner.parse(statement)
    val result    = RelRunners.run(plan.relOpt).executeQuery()
    // some special operations
    plan.sqlNode match {
      case node: SqlUseDatabase =>
        useSchema = node.useDatabase
      case _ =>
    }

    DefaultQueryResult(result, useSchema)
  }
}

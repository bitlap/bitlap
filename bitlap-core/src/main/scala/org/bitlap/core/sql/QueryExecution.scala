/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.core.sql

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
      case e if e != null     => throw new BitlapException(statement, Map.empty[String, String], Some(e))
      case e                  => throw new BitlapException(statement, Map.empty[String, String], None)
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

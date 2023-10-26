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

import java.io.Serializable

import org.bitlap.common.BitlapConf

import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlSelect

/** Query thread context for each query statement.
 */
class QueryContext extends Serializable {

  var runtimeConf: BitlapConf      = _
  var currentSchema: String        = _
  var statement: String            = _
  var originalPlan: SqlNode        = _
  var currentSelectNode: SqlSelect = _
  var queryId: String              = _
}

object QueryContext {

  private val context =
    new ThreadLocal[QueryContext]() {
      override def initialValue(): QueryContext = QueryContext()
    }

  def get(): QueryContext = context.get()

  private def reset(): Unit = context.remove()

  def use[T](block: (QueryContext) => T): T = {
    try {
      reset()
      block(get())
    } finally {
      reset()
    }
  }
}

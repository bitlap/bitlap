/** Copyright (C) 2023 bitlap.org .
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

  def reset() = context.remove()
  
  def use[T](block: (QueryContext) => T): T = {
    try {
      reset()
      return block(get())
    } finally {
      reset()
    }
  }
}

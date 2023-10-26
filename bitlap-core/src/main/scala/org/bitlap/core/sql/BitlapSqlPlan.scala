/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql

import org.apache.calcite.rel.RelNode
import org.apache.calcite.sql.SqlNode

/** sql plan
 */
final case class BitlapSqlPlan(
  statement: String,
  sqlNode: SqlNode,
  rel: RelNode,
  relOpt: RelNode // optimized rel
) {

  def explain(): String = {
    this.relOpt.explain()
  }
}

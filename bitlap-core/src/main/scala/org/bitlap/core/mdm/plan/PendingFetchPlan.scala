/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.mdm.plan

import org.bitlap.core.catalog.metadata.Table
import org.bitlap.core.mdm.{ FetchContext, FetchPlan }
import org.bitlap.core.mdm.model.RowIterator
import org.bitlap.core.sql.MDColumnAnalyzer
import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.PruneTimeFilter

/** Desc: Pending fetch plan to be analyzed
 */
class PendingFetchPlan(
  val table: Table,
  val analyzer: MDColumnAnalyzer,
  val timeFilter: PruneTimeFilter,
  val pushedFilter: PrunePushedFilter,
  override val subPlans: List[FetchPlan] = List.empty[FetchPlan])
    extends FetchPlan {

  override def execute(context: FetchContext): RowIterator = {
    throw IllegalStateException(s"Bitlap PendingFetchPlan cannot be executed")
  }

  override def explain(depth: Int): String = {
    s"${" ".repeat(depth)}+- PendingFetchPlan"
  }
}

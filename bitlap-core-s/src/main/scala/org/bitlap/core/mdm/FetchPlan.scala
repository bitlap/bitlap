/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.mdm

import org.bitlap.core.mdm.model.RowIterator

/** Fetch plan
 */
trait FetchPlan {

  /** sub plans
   */
  val subPlans: List[FetchPlan]

  /** if optimized for current plan
   */
  var optimized: Boolean = false

  /** execute current plan
   */
  def execute(context: FetchContext): RowIterator

  /** execute plan graph
   */
  def explain(depth: Int = 0): String

  /** fetch data with computed fetcher.
   */
  def withFetcher[R](context: FetchContext)(fetch: (Fetcher) => R): R = {
    return fetch(context.findBestFetcher(this))
  }
}

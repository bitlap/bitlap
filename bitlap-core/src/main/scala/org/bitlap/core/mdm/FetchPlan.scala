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
    fetch(context.findBestFetcher(this))
  }
}

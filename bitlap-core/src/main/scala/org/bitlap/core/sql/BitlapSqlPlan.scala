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

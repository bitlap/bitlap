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
package org.bitlap.core.sql.udf

object UDFNames {

  // common
  val hello = "hello"
  val `if`  = "if"

  // date
  val date_format = "date_format"

  // bitmap
  val bm_sum_aggr       = "bm_sum"
  val bm_count_aggr     = "bm_count"
  val bm_count_distinct = "bm_count_distinct"
}

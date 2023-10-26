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

import org.apache.calcite.sql.`type`.SqlReturnTypeInference
import org.apache.calcite.sql.`type`.SqlTypeName

/** A generic interface for defining user-defined aggregate functions.
 *
 *  [A]: accumulator type [V]: input value type [R]: result type
 */
trait UDAF[A, V, R] extends UDF {

  /** agg function name
   */
  override val name: String

  /** input types
   */
  override val inputTypes: List[SqlTypeName]

  /** agg result type
   */
  override val resultType: SqlReturnTypeInference

  /** agg init value
   */
  def init(): A

  /** add one input to accumulator
   */
  def add(accumulator: A, input: V): A

  /** merge two accumulator
   */
  def merge(accumulator1: A, accumulator2: A): A

  /** agg result
   */
  def result(accumulator: A): R
}

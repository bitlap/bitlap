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

/** A generic interface for defining user-defined functions.
 *
 *  [V1]: input value type [V2]: input value type [V3]: input value type [R]: result type
 */
trait UDF3[V1, V2, V3, R] extends UDF {

  /** eval with three inputs.
   */
  def eval(input1: V1, input2: V2, input3: V3): R
}

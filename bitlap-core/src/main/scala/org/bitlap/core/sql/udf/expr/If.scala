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
package org.bitlap.core.sql.udf.expr

import org.bitlap.core.sql.udf.UDF3
import org.bitlap.core.sql.udf.UDFNames

import org.apache.calcite.sql.`type`.ReturnTypes
import org.apache.calcite.sql.`type`.SqlReturnTypeInference
import org.apache.calcite.sql.`type`.SqlTypeName

/** IF UDF
 *
 *  expression: if(condition, expr1, expr2) return: if condition is true then $expr1, else $expr2
 */
class If extends UDF3[Boolean, Any, Any, Any] {

  override val name: String                       = UDFNames.`if`
  override val inputTypes: List[SqlTypeName]      = List(SqlTypeName.BOOLEAN, SqlTypeName.ANY, SqlTypeName.ANY)
  override val resultType: SqlReturnTypeInference = ReturnTypes.ARG1_NULLABLE

  override def eval(input1: Boolean, input2: Any, input3: Any): Any = {
    if (input1) {
      return input2
    }
    input3
  }
}

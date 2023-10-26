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
package org.bitlap.core.sql.udf.date

import org.bitlap.core.sql.infer
import org.bitlap.core.sql.udf.UDF2
import org.bitlap.core.sql.udf.UDFNames

import org.apache.calcite.sql.`type`.SqlReturnTypeInference
import org.apache.calcite.sql.`type`.SqlTypeName
import org.joda.time.DateTime

/** date_format UDF
 *
 *  expression: date_format(time, pattern)
 */
class DateFormat extends UDF2[Any, String, String] {

  override val name: String                       = UDFNames.date_format
  override val inputTypes: List[SqlTypeName]      = List(SqlTypeName.ANY, SqlTypeName.VARCHAR)
  override val resultType: SqlReturnTypeInference = SqlTypeName.VARCHAR.infer()

  override def eval(input1: Any, input2: String): String = {
    input1 match {
      case null => null
      case _    => DateTime(input1).toString(input2)
    }
  }
}

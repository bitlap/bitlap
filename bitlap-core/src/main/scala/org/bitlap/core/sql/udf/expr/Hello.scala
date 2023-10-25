/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.udf.expr

import org.bitlap.core.sql.infer
import org.bitlap.core.sql.udf.UDF1
import org.bitlap.core.sql.udf.UDFNames

import org.apache.calcite.sql.`type`.SqlReturnTypeInference
import org.apache.calcite.sql.`type`.SqlTypeName

/** Hello UDF
 *
 *  expression: hello(expr) return: hello $expr
 */
class Hello extends UDF1[Any, String] {

  override val name: String                       = UDFNames.hello
  override val inputTypes: List[SqlTypeName]      = List(SqlTypeName.ANY)
  override val resultType: SqlReturnTypeInference = SqlTypeName.VARCHAR.infer()

  override def eval(input: Any): String = {
    if (input == null) {
      return null
    }
    s"hello $input"
  }
}

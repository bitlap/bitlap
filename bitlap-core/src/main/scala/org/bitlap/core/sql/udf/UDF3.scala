/**
 * Copyright (C) 2023 bitlap.org .
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

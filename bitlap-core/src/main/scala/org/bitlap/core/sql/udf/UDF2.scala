/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.udf

/** A generic interface for defining user-defined functions.
 *
 *  [V1]: input value type [V2]: input value type [R]: result type
 */
trait UDF2[V1, V2, R] extends UDF {

  /** eval with two inputs.
   */
  def eval(input1: V1, input2: V2): R
}

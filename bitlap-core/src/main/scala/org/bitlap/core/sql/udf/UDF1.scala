/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.udf

/** A generic interface for defining user-defined functions.
 *
 *  [V]: input value type [R]: result type
 */
trait UDF1[V, R] extends UDF {

  /** eval with one input.
   */
  def eval(input: V): R
}

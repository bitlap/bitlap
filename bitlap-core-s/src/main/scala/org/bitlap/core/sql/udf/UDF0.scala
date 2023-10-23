/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.udf

/** A generic interface for defining user-defined functions.
 *
 *  [R]: result type
 */
trait UDF0[R] extends UDF {

  /** eval with no inputs.
   */
  def eval(): R
}

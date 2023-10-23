/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql

/** op enum for filter expression
 */
enum FilterOp(val op: String) {
  case EQUALS              extends FilterOp("=")
  case NOT_EQUALS          extends FilterOp("<>")
  case GREATER_THAN        extends FilterOp(">")
  case GREATER_EQUALS_THAN extends FilterOp(">=")
  case LESS_THAN           extends FilterOp("<")
  case LESS_EQUALS_THAN    extends FilterOp("<=")
  case OPEN                extends FilterOp("()")
  case CLOSED              extends FilterOp("[]")
  case CLOSED_OPEN         extends FilterOp("[)")
  case OPEN_CLOSED         extends FilterOp("(]")

}

object FilterOp {

  def from(op: String): FilterOp = {
    op match
      case "!=" => NOT_EQUALS
      case _    => values.find(_.op == op).get
  }
}

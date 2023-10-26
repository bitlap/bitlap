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

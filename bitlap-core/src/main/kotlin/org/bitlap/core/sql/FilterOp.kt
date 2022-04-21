/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.sql

/**
 * op enum for filter expression
 */
enum class FilterOp(val op: String) {
    EQUALS("="), NOT_EQUALS("<>"),
    GREATER_THAN(">"), GREATER_EQUALS_THAN(">="),
    LESS_THAN("<"), LESS_EQUALS_THAN("<="),
    OPEN("()"), CLOSED("[]"), CLOSED_OPEN("[)"), OPEN_CLOSED("(]");

    companion object {
        fun from(op: String): FilterOp {
            return when (op) {
                "!=" -> NOT_EQUALS
                else -> values().find { it.op == op }!!
            }
        }
    }
}

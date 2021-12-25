package org.bitlap.core.sql.udf

/**
 * built-in user defined functions
 */
object UDFs {

    @JvmStatic
    fun condition(condition: Boolean, a: Any, b: Any): Any {
        if (condition) {
            return a
        }
        return b
    }
}

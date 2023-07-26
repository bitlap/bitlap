/* Copyright (c) 2023 bitlap.org */
package org.bitlap.common.exception

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

/**
 * get errors info from classpath `bitlap-errors.conf`
 */
val errors: Config by lazy {
    ConfigFactory.load("bitlap-errors")
}

/**
 * format message of [this] key path with [parameter]
 */
fun String?.formatErrorMessage(parameter: Map<String, String>): String {
    if (this.isNullOrBlank()) return ""
    return runCatching { errors.getConfig(this) }
        .mapCatching {
            parameter.entries.fold(it.getString("message")) { acc, (k, v) ->
                acc.replace("<$k>", v)
            }
        }
        .getOrElse { this }
}

/**
 * format sql state of [this] key path
 */
fun String?.formatSqlState(): String {
    if (this.isNullOrBlank()) return ""
    return runCatching { errors.getConfig(this) }
        .mapCatching {
            it.getString("sqlState")
        }
        .getOrElse { "" }
}

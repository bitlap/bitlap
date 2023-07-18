/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.catalog.metadata

/**
 * Database
 */
data class Database(private val _name: String) {
    val name: String
        get() = _name.lowercase()

    companion object {
        const val DEFAULT_DATABASE = "default"
    }
}
